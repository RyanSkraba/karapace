"""
karapace - conftest

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from dataclasses import dataclass
from kafka import KafkaAdminClient, KafkaProducer
from karapace.avro_compatibility import SchemaCompatibilityResult
from karapace.config import set_config_defaults, write_config
from karapace.kafka_rest_apis import KafkaRest, KafkaRestAdminClient
from karapace.schema_registry_apis import KarapaceSchemaRegistry
from tests.utils import (
    Client, client_for, get_broker_ip, KafkaConfig, mock_factory, new_random_name, REGISTRY_URI, REST_URI, TempDirCreator
)
from typing import AsyncIterator, Dict, Iterator, List, Optional, Tuple

import os
import pytest
import random
import signal
import socket
import subprocess
import time

KAFKA_CURRENT_VERSION = "2.4"
BASEDIR = "kafka_2.12-2.4.1"


@dataclass(frozen=True)
class PortRangeInclusive:
    start: int
    end: int

    PRIVILEGE_END = 2 ** 10
    MAX_PORTS = 2 ** 16 - 1

    def __post_init__(self):
        # Make sure the range is valid and that we don't need to be root
        assert self.end > self.start, "there must be at least one port available"
        assert self.end <= self.MAX_PORTS, f"end must be lower than {self.MAX_PORTS}"
        assert self.start > self.PRIVILEGE_END, "start must not be a privileged port"

    def next_range(self, number_of_ports: int) -> "PortRangeInclusive":
        next_start = self.end + 1
        next_end = next_start + number_of_ports - 1  # -1 because the range is inclusive

        return PortRangeInclusive(next_start, next_end)


# To find a good port range use the following:
#
#   curl --silent 'https://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.txt' | \
#       egrep -i -e '^\s*[0-9]+-[0-9]+\s*unassigned' | \
#       awk '{print $1}'
#
KAFKA_PORTS = PortRangeInclusive(48700, 48800)
ZK_PORT_RANGE = KAFKA_PORTS.next_range(100)
REGISTRY_PORT_RANGE = ZK_PORT_RANGE.next_range(100)


class Timeout(Exception):
    pass


@dataclass(frozen=True)
class Expiration:
    msg: str
    deadline: float

    @classmethod
    def from_timeout(cls, msg: str, timeout: float):
        return cls(msg, time.monotonic() + timeout)

    def raise_if_expired(self):
        if time.monotonic() > self.deadline:
            raise Timeout(self.msg)


@dataclass
class ZKConfig:
    client_port: int
    admin_port: int
    path: str


def pytest_assertrepr_compare(op, left, right) -> Optional[List[str]]:
    if isinstance(left, SchemaCompatibilityResult) and isinstance(right, SchemaCompatibilityResult) and op in ("==", "!="):
        lines = ["Comparing SchemaCompatibilityResult instances:"]

        def pad(depth: int, *msg: str) -> str:
            return "  " * depth + ' '.join(msg)

        def list_details(header: str, depth: int, items: List[str]) -> None:
            qty = len(items)

            if qty == 1:
                lines.append(pad(depth, header, *items))
            elif qty > 1:
                lines.append(pad(depth, header))
                depth += 1
                for loc in items:
                    lines.append(pad(depth, loc))

        def compatibility_details(header: str, depth: int, obj: SchemaCompatibilityResult) -> None:
            lines.append(pad(depth, header))

            depth += 1

            lines.append(pad(depth, 'compatibility', str(obj.compatibility)))
            list_details('locations:', depth, list(obj.locations))
            list_details('messages:', depth, list(obj.messages))
            list_details('incompatibilities:', depth, [str(i) for i in obj.incompatibilities])

        depth = 1
        compatibility_details("Left:", depth, left)
        compatibility_details("Right:", depth, right)
        return lines

    return None


def port_is_listening(hostname: str, port: int, ipv6: bool) -> bool:
    if ipv6:
        s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM, 0)
    else:
        s = socket.socket()
    s.settimeout(0.5)
    try:
        s.connect((hostname, port))
        s.close()
        return True
    except socket.error:
        return False


def wait_for_kafka(port: int, *, hostname: str = "127.0.0.1", wait_time: float = 20.0) -> None:
    bootstrap_server = f"{hostname}:{port}"
    expiration = Expiration.from_timeout(
        msg=f"Could not contact kafka cluster on host `{bootstrap_server}`",
        timeout=wait_time,
    )

    list_topics_successful = False
    while not list_topics_successful:
        expiration.raise_if_expired()
        try:
            KafkaAdminClient(bootstrap_servers=bootstrap_server).list_topics()
        except Exception as e:  # pylint: disable=broad-except
            print(f"Error checking kafka cluster: {e}")
            time.sleep(2.0)
        else:
            list_topics_successful = True


def wait_for_port(port: int, *, hostname: str = "127.0.0.1", wait_time: float = 20.0, ipv6: bool = False) -> None:
    start_time = time.monotonic()
    expiration = Expiration(
        msg=f"Timeout waiting for `{hostname}:{port}`",
        deadline=start_time + wait_time,
    )

    while not port_is_listening(hostname, port, ipv6):
        expiration.raise_if_expired()
        time.sleep(2.0)

    elapsed = time.monotonic() - start_time
    print(f"Server `{hostname}:{port}` listening after {elapsed} seconds")


def get_random_port(*, port_range: PortRangeInclusive, blacklist: List[int]) -> int:
    """ Find a random port in the range `PortRangeInclusive`.

    Note:
        This function is *not* aware of the ports currently open in the system,
        the blacklist only prevents two services of the same type to randomly
        get the same ports for *a single test run*.

        Because of that, the port range should be chosen such that there is no
        system service in the range. Also note that running two sessions of the
        tests with the same range is not supported and will lead to flakiness.
    """
    value = random.randint(port_range.start, port_range.end)
    while value in blacklist:
        value = random.randint(port_range.start, port_range.end)
    return value


@pytest.fixture(scope="session", name="zkserver")
def fixture_zkserver(session_tmpdir: TempDirCreator) -> Iterator[Optional[ZKConfig]]:
    if REGISTRY_URI in os.environ or REST_URI in os.environ:
        yield None
    else:
        config, proc = zkserver_base(session_tmpdir)
        try:
            yield config
        finally:
            time.sleep(5)
            os.kill(proc.pid, signal.SIGKILL)
            proc.wait(timeout=10.0)


@pytest.fixture(scope="session", name="kafka_server")
def fixture_kafka_server(session_tmpdir: TempDirCreator, zkserver: ZKConfig) -> Iterator[Optional[KafkaConfig]]:
    if REGISTRY_URI in os.environ or REST_URI in os.environ:
        yield None
    else:
        config, proc = kafka_server_base(session_tmpdir, zkserver)
        try:
            yield config
        finally:
            time.sleep(5)
            os.kill(proc.pid, signal.SIGKILL)
            proc.wait(timeout=10.0)


@pytest.fixture(scope="function", name="producer")
def fixture_producer(kafka_server: Optional[KafkaConfig]) -> KafkaProducer:
    if not kafka_server:
        assert REST_URI in os.environ or REGISTRY_URI in os.environ
        kafka_uri = f"{get_broker_ip()}:9092"
    else:
        kafka_uri = "127.0.0.1:{}".format(kafka_server.kafka_port)
    prod = KafkaProducer(bootstrap_servers=kafka_uri)
    try:
        yield prod
    finally:
        prod.close()


@pytest.fixture(scope="function", name="admin_client")
def fixture_admin(kafka_server: Optional[KafkaConfig]) -> Iterator[KafkaRestAdminClient]:
    if not kafka_server:
        assert REST_URI in os.environ or REGISTRY_URI in os.environ
        kafka_uri = f"{get_broker_ip()}:9092"
    else:
        kafka_uri = "127.0.0.1:{}".format(kafka_server.kafka_port)
    cli = KafkaRestAdminClient(bootstrap_servers=kafka_uri)
    try:
        yield cli
    finally:
        cli.close()


@pytest.fixture(scope="function", name="rest_async")
async def fixture_rest_async(
    session_tmpdir: TempDirCreator, kafka_server: Optional[KafkaConfig], registry_async_client: Client
) -> AsyncIterator[KafkaRest]:
    if not kafka_server:
        assert REST_URI in os.environ
        instance, _ = mock_factory("rest")()
        yield instance
    else:
        config_path = os.path.join(session_tmpdir(), "karapace_config.json")
        kafka_port = kafka_server.kafka_port

        config = set_config_defaults({
            "log_level": "WARNING",
            "bootstrap_uri": f"127.0.0.1:{kafka_port}",
            "admin_metadata_max_age": 0
        })
        write_config(config_path, config)
        rest = KafkaRest(config_file_path=config_path, config=config)

        assert rest.serializer.registry_client
        assert rest.consumer_manager.deserializer.registry_client
        rest.serializer.registry_client.client = registry_async_client
        rest.consumer_manager.deserializer.registry_client.client = registry_async_client
        try:
            yield rest
        finally:
            rest.close()
            await rest.close_producers()


@pytest.fixture(scope="function", name="rest_async_client")
async def fixture_rest_async_client(rest_async: KafkaRest, aiohttp_client) -> AsyncIterator[Client]:
    cli = await client_for(rest_async, aiohttp_client)
    yield cli
    await cli.close()


@pytest.fixture(scope="function", name="registry_async_pair")
def fixture_registry_async_pair(session_tmpdir: TempDirCreator, kafka_server: Optional[KafkaConfig]):
    assert kafka_server, f"registry_async_pair can not be used if the env variable `{REGISTRY_URI}` or `{REST_URI}` is set"

    master_config_path = os.path.join(session_tmpdir(), "karapace_config_master.json")
    slave_config_path = os.path.join(session_tmpdir(), "karapace_config_slave.json")
    master_port = get_random_port(port_range=REGISTRY_PORT_RANGE, blacklist=[])
    slave_port = get_random_port(port_range=REGISTRY_PORT_RANGE, blacklist=[master_port])
    kafka_port = kafka_server.kafka_port
    topic_name = new_random_name("schema_pairs")
    group_id = new_random_name("schema_pairs")
    write_config(
        master_config_path, {
            "log_level": "WARNING",
            "bootstrap_uri": f"127.0.0.1:{kafka_port}",
            "topic_name": topic_name,
            "group_id": group_id,
            "advertised_hostname": "127.0.0.1",
            "karapace_registry": True,
            "port": master_port,
        }
    )
    write_config(
        slave_config_path, {
            "log_level": "WARNING",
            "bootstrap_uri": f"127.0.0.1:{kafka_port}",
            "topic_name": topic_name,
            "group_id": group_id,
            "advertised_hostname": "127.0.0.1",
            "karapace_registry": True,
            "port": slave_port,
        }
    )
    master_process = subprocess.Popen(["python", "-m", "karapace.karapace_all", master_config_path])
    slave_process = subprocess.Popen(["python", "-m", "karapace.karapace_all", slave_config_path])
    try:
        wait_for_port(master_port)
        wait_for_port(slave_port)
        yield f"http://127.0.0.1:{master_port}", f"http://127.0.0.1:{slave_port}"
    finally:
        master_process.kill()
        slave_process.kill()


@pytest.fixture(scope="function", name="registry_async")
async def fixture_registry_async(session_tmpdir: TempDirCreator,
                                 kafka_server: Optional[KafkaConfig]) -> AsyncIterator[KarapaceSchemaRegistry]:
    if not kafka_server:
        assert REGISTRY_URI in os.environ or REST_URI in os.environ
        instance, _ = mock_factory("registry")()
        yield instance
    else:
        config_path = os.path.join(session_tmpdir(), "karapace_config.json")
        kafka_port = kafka_server.kafka_port

        config = set_config_defaults({
            "log_level": "WARNING",
            "bootstrap_uri": f"127.0.0.1:{kafka_port}",
            "topic_name": new_random_name(),
            "group_id": new_random_name("schema_registry")
        })
        write_config(config_path, config)
        registry = KarapaceSchemaRegistry(config_file_path=config_path, config=set_config_defaults(config))
        await registry.get_master()
        try:
            yield registry
        finally:
            registry.close()


@pytest.fixture(scope="function", name="registry_async_client")
async def fixture_registry_async_client(registry_async: KarapaceSchemaRegistry, aiohttp_client) -> AsyncIterator[Client]:
    cli = await client_for(registry_async, aiohttp_client)
    yield cli
    await cli.close()


def kafka_java_args(heap_mb, kafka_config_path, logs_dir, log4j_properties_path):
    java_args = [
        "-Xmx{}M".format(heap_mb),
        "-Xms{}M".format(heap_mb),
        "-Dkafka.logs.dir={}/logs".format(logs_dir),
        "-Dlog4j.configuration=file:{}".format(log4j_properties_path),
        "-cp",
        ":".join([
            os.path.join(BASEDIR, "libs", "*"),
        ]),
        "kafka.Kafka",
        kafka_config_path,
    ]
    return java_args


def get_java_process_configuration(java_args: List[str]) -> List[str]:
    command = [
        "/usr/bin/java",
        "-server",
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=20",
        "-XX:InitiatingHeapOccupancyPercent=35",
        "-XX:+DisableExplicitGC",
        "-XX:+ExitOnOutOfMemoryError",
        "-Djava.awt.headless=true",
        "-Dcom.sun.management.jmxremote",
        "-Dcom.sun.management.jmxremote.authenticate=false",
        "-Dcom.sun.management.jmxremote.ssl=false",
    ]
    command.extend(java_args)
    return command


def kafka_server_base(session_tmpdir: TempDirCreator, zk: ZKConfig) -> Tuple[KafkaConfig, subprocess.Popen]:
    datadir = session_tmpdir()
    plaintext_port = get_random_port(port_range=KAFKA_PORTS, blacklist=[])

    config = KafkaConfig(
        datadir=datadir.join("data").strpath,
        kafka_keystore_password="secret",
        kafka_port=plaintext_port,
        zookeeper_port=zk.client_port,
    )

    os.makedirs(config.datadir)
    advertised_listeners = ",".join([
        "PLAINTEXT://127.0.0.1:{}".format(plaintext_port),
    ])
    listeners = ",".join([
        "PLAINTEXT://:{}".format(plaintext_port),
    ])

    kafka_config = {
        "broker.id": 1,
        "broker.rack": "local",
        "advertised.listeners": advertised_listeners,
        "auto.create.topics.enable": False,
        "default.replication.factor": 1,
        "delete.topic.enable": "true",
        "inter.broker.listener.name": "PLAINTEXT",
        "inter.broker.protocol.version": KAFKA_CURRENT_VERSION,
        "listeners": listeners,
        "log.cleaner.enable": "true",
        "log.dirs": config.datadir,
        "log.message.format.version": KAFKA_CURRENT_VERSION,
        "log.retention.check.interval.ms": 300000,
        "log.segment.bytes": 200 * 1024 * 1024,  # 200 MiB
        "num.io.threads": 8,
        "num.network.threads": 112,
        "num.partitions": 1,
        "num.replica.fetchers": 4,
        "num.recovery.threads.per.data.dir": 1,
        "offsets.topic.replication.factor": 1,
        "socket.receive.buffer.bytes": 100 * 1024,
        "socket.request.max.bytes": 100 * 1024 * 1024,
        "socket.send.buffer.bytes": 100 * 1024,
        "transaction.state.log.min.isr": 1,
        "transaction.state.log.num.partitions": 16,
        "transaction.state.log.replication.factor": 1,
        "zookeeper.connection.timeout.ms": 6000,
        "zookeeper.connect": "{}:{}".format("127.0.0.1", zk.client_port)
    }
    kafka_config_path = os.path.join(datadir.strpath, "kafka", "config")
    os.makedirs(kafka_config_path)

    kafka_config_path = os.path.join(kafka_config_path, "server.properties")
    with open(kafka_config_path, "w") as fp:
        for key, value in kafka_config.items():
            fp.write("{}={}\n".format(key, value))

    log4j_properties_path = os.path.join(BASEDIR, "config/log4j.properties")

    kafka_cmd = get_java_process_configuration(
        java_args=kafka_java_args(
            heap_mb=256,
            logs_dir=os.path.join(datadir.strpath, "kafka"),
            log4j_properties_path=log4j_properties_path,
            kafka_config_path=kafka_config_path,
        ),
    )
    env: Dict[bytes, bytes] = dict()
    proc = subprocess.Popen(kafka_cmd, env=env)
    wait_for_kafka(config.kafka_port, wait_time=60)
    return config, proc


def zkserver_base(session_tmpdir: TempDirCreator, subdir: str = "base") -> Tuple[ZKConfig, subprocess.Popen]:
    datadir = session_tmpdir()
    path = os.path.join(datadir.strpath, subdir)
    os.makedirs(path)

    client_port = get_random_port(port_range=ZK_PORT_RANGE, blacklist=[])
    admin_port = get_random_port(port_range=ZK_PORT_RANGE, blacklist=[client_port])
    config = ZKConfig(
        client_port=client_port,
        admin_port=admin_port,
        path=path,
    )
    zoo_cfg = """
# The number of milliseconds of each tick
tickTime=2000
# The number of ticks that the initial
# synchronization phase can take
initLimit=10
# The number of ticks that can pass between
# sending a request and getting an acknowledgement
syncLimit=5
# the directory where the snapshot is stored.
# do not use /tmp for storage, /tmp here is just
# example sakes.
dataDir={path}
# the port at which the clients will connect
clientPort={client_port}
#clientPortAddress=127.0.0.1
# the maximum number of client connections.
# increase this if you need to handle more clients
#maxClientCnxns=60
#
# Be sure to read the maintenance section of the
# administrator guide before turning on autopurge.
#
# http://zookeeper.apache.org/doc/current/zookeeperAdmin.html#sc_maintenance
#
# The number of snapshots to retain in dataDir
#autopurge.snapRetainCount=3
# Purge task interval in hours
# Set to "0" to disable auto purge feature
#autopurge.purgeInterval=1
# admin server
admin.serverPort={admin_port}
admin.enableServer=false
# Allow reconfig calls to be made to add/remove nodes to the cluster on the fly
reconfigEnabled=true
# Don't require authentication for reconfig
skipACL=yes
""".format(
        client_port=config.client_port,
        admin_port=config.admin_port,
        path=config.path,
    )
    zoo_cfg_path = os.path.join(path, "zoo.cfg")
    with open(zoo_cfg_path, "w") as fp:
        fp.write(zoo_cfg)
    env = {
        "CLASSPATH": "/usr/share/java/slf4j/slf4j-simple.jar",
        "ZOO_LOG_DIR": datadir.join("logs").strpath,
    }
    java_args = get_java_process_configuration(
        java_args=[
            "-cp", ":".join([
                os.path.join(BASEDIR, "libs", "*"),
            ]), "org.apache.zookeeper.server.quorum.QuorumPeerMain", zoo_cfg_path
        ]
    )
    proc = subprocess.Popen(java_args, env=env)
    wait_for_port(config.client_port)
    return config, proc