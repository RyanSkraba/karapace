"""
karapace - test schema backup

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from karapace.config import set_config_defaults
from karapace.schema_backup import SchemaBackup
from karapace.utils import Client
from pathlib import Path
from tests.utils import KafkaServers
from typing import Any, Dict

import os
import ujson

baseurl = "http://localhost:8081"


JSON_SUBJECT = "json-schemas"
JSON_SUBJECT_HASH = "a2a0483c6ce0d38798ef218420e3f132608dbebf"
JSON_SCHEMA = {
    "type": "object",
    "title": "JSON-schema",
    "description": "example",
    "properties": {"test": {"type": "integer", "title": "my test number", "default": 5}},
}

AVRO_SUBJECT = "avro-schemas"
AVRO_SUBJECT_HASH = "a801beafef1fb8c03907b44ec7baca341a58420d"
AVRO_SCHEMA = {
    "type": "record",
    "namespace": "io.aiven",
    "name": "myrecord",
    "fields": [
        {
            "type": "string",
            "name": "f1",
        },
    ],
}
EXPECTED_AVRO_SCHEMA = {
    "type": "record",
    "namespace": "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382",
    "name": "afe8733e983101f1f4ff50d24152890d0da71418",
    "fields": [
        {
            "type": "string",
            "name": "a09bb890b096f7306f688cc6d1dad34e7e52a223",
        },
    ],
}


async def insert_data(c: Client, schemaType: str, subject: str, data: Dict[str, Any]) -> None:
    schema_string = ujson.dumps(data)
    res = await c.post(
        "subjects/{}/versions".format(subject),
        json={"schema": f"{schema_string}", "schemaType": schemaType},
    )
    assert res.status == 200
    assert "id" in res.json()


async def test_export_anonymized_avro_schemas(
    registry_async_client: Client, kafka_servers: KafkaServers, tmp_path: Path
) -> None:
    await insert_data(registry_async_client, "JSON", JSON_SUBJECT, JSON_SCHEMA)
    await insert_data(registry_async_client, "AVRO", AVRO_SUBJECT, AVRO_SCHEMA)

    # Get the backup
    export_location = tmp_path / "export.log"
    config = set_config_defaults({"bootstrap_uri": kafka_servers.bootstrap_servers})
    sb = SchemaBackup(config, str(export_location))
    sb.export_anonymized_avro_schemas()

    # The export file has been created
    assert os.path.exists(export_location)

    expected_subject_hash_found = False
    json_schema_subject_hash_found = False
    with export_location.open("r") as fp:
        exported_data = ujson.load(fp)
        for msg in exported_data:
            assert len(msg) == 2
            key = msg[0]
            subject_hash = key.get("subject", None)
            if subject_hash == AVRO_SUBJECT_HASH:
                expected_subject_hash_found = True
                schema_data = msg[1]

                assert schema_data["subject"] == AVRO_SUBJECT_HASH
                assert schema_data["schema"] == EXPECTED_AVRO_SCHEMA
            if subject_hash == JSON_SUBJECT_HASH:
                json_schema_subject_hash_found = True

    assert expected_subject_hash_found
    assert not json_schema_subject_hash_found
