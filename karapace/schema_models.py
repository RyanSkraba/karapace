"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from avro.errors import SchemaParseException
from avro.schema import parse as avro_parse, Schema as AvroSchema
from jsonschema import Draft7Validator
from jsonschema.exceptions import SchemaError
from karapace.dependency import Dependency
from karapace.errors import InvalidSchema
from karapace.protobuf.exception import (
    Error as ProtobufError,
    IllegalArgumentException,
    IllegalStateException,
    ProtobufException,
    ProtobufParserRuntimeException,
    ProtobufUnresolvedDependencyException,
    SchemaParseException as ProtobufSchemaParseException,
)
from karapace.protobuf.schema import ProtobufSchema
from karapace.schema_references import Reference
from karapace.schema_type import SchemaType
from karapace.utils import json_encode
from typing import Any, cast, Dict, List, Optional, Union

import json


def parse_avro_schema_definition(s: str, validate_enum_symbols: bool = True, validate_names: bool = True) -> AvroSchema:
    """Compatibility function with Avro which ignores trailing data in JSON
    strings.

    The Python stdlib `json` module doesn't allow to ignore trailing data. If
    parsing fails because of it, the extra data can be removed and parsed
    again.
    """
    try:
        json_data = json.loads(s)
    except json.JSONDecodeError as e:
        if e.msg != "Extra data":
            raise

        json_data = json.loads(s[: e.pos])

    return avro_parse(json.dumps(json_data), validate_enum_symbols=validate_enum_symbols, validate_names=validate_names)


def parse_jsonschema_definition(schema_definition: str) -> Draft7Validator:
    """Parses and validates `schema_definition`.

    Raises:
        SchemaError: If `schema_definition` is not a valid Draft7 schema.
    """
    schema = json.loads(schema_definition)
    Draft7Validator.check_schema(schema)
    return Draft7Validator(schema)


def parse_protobuf_schema_definition(
    schema_definition: str,
    references: Optional[List[Reference]] = None,
    dependencies: Optional[Dict[str, Dependency]] = None,
    validate_references: bool = True,
) -> ProtobufSchema:
    """Parses and validates `schema_definition`.

    Raises:
        Nothing yet.

    """
    protobuf_schema = ProtobufSchema(schema_definition, references, dependencies)
    if validate_references:
        result = protobuf_schema.verify_schema_dependencies()
        if not result.result:
            raise ProtobufUnresolvedDependencyException(f"{result.message}")
    return protobuf_schema


class TypedSchema:
    def __init__(
        self,
        *,
        schema_type: SchemaType,
        schema_str: str,
        schema: Optional[Union[Draft7Validator, AvroSchema, ProtobufSchema]] = None,
        references: Optional[List[Reference]] = None,
        dependencies: Optional[Dict[str, Dependency]] = None,
    ):
        """Schema with type information

        Args:
            schema_type (SchemaType): The type of the schema
            schema_str (str): The original schema string
            schema (Optional[Union[Draft7Validator, AvroSchema, ProtobufSchema]]): The parsed and validated schema
            references (Optional[List[Dependency]]): The references of schema
        """
        self.schema_type = schema_type
        self.schema_str = schema_str
        self.references = references
        self.dependencies = dependencies
        self.max_id: Optional[int] = None

        self._str_cached: Optional[str] = None
        self._schema_cached: Optional[Union[Draft7Validator, AvroSchema, ProtobufSchema]] = schema

    def to_dict(self) -> Dict[str, Any]:
        if self.schema_type is SchemaType.PROTOBUF:
            raise InvalidSchema("Protobuf do not support to_dict serialization")
        return json.loads(self.schema_str)

    @property
    def schema(self) -> Union[Draft7Validator, AvroSchema, ProtobufSchema]:
        if self._schema_cached is not None:
            return self._schema_cached
        if self.schema_type is SchemaType.AVRO:
            try:
                self._schema_cached = parse_avro_schema_definition(
                    self.schema_str, validate_enum_symbols=False, validate_names=False
                )
            except (SchemaParseException, json.JSONDecodeError, TypeError) as e:
                raise InvalidSchema from e

        elif self.schema_type is SchemaType.JSONSCHEMA:
            try:
                self._schema_cached = parse_jsonschema_definition(self.schema_str)
                # TypeError - Raised when the user forgets to encode the schema as a string.
            except (TypeError, json.JSONDecodeError, SchemaError, AssertionError) as e:
                raise InvalidSchema from e

        elif self.schema_type is SchemaType.PROTOBUF:
            try:
                self._schema_cached = parse_protobuf_schema_definition(self.schema_str, self.references, self.dependencies)
            except (
                TypeError,
                SchemaError,
                AssertionError,
                ProtobufParserRuntimeException,
                IllegalStateException,
                IllegalArgumentException,
                ProtobufError,
                ProtobufException,
                ProtobufSchemaParseException,
            ) as e:
                raise InvalidSchema from e
        else:
            raise InvalidSchema(f"Unknown parser {self.schema_type} for {self.schema_str}")
        return self._schema_cached

    def get_references(self) -> Optional[List[Reference]]:
        return self.references

    def __str__(self) -> str:
        if self.schema_type == SchemaType.PROTOBUF:
            return self.schema_str

        if self._str_cached is None:
            self._str_cached = json_encode(self.to_dict())
        return self._str_cached

    def __repr__(self) -> str:
        return f"TypedSchema(type={self.schema_type}, schema={str(self)})"

    def __eq__(self, other: Any) -> bool:
        schema_is_equal = (
            isinstance(other, (TypedSchema, ValidatedTypedSchema))
            and self.schema_type is other.schema_type
            and self.__str__() == other.__str__()
        )
        if not schema_is_equal:
            return False
        return self.references == other.references


def parse(
    schema_type: SchemaType,
    schema_str: str,
    validate_avro_enum_symbols: bool,
    validate_avro_names: bool,
    references: Optional[List[Reference]] = None,
    dependencies: Optional[Dict[str, Dependency]] = None,
) -> "ParsedTypedSchema":
    if schema_type not in [SchemaType.AVRO, SchemaType.JSONSCHEMA, SchemaType.PROTOBUF]:
        raise InvalidSchema(f"Unknown parser {schema_type} for {schema_str}")

    parsed_schema: Union[Draft7Validator, AvroSchema, ProtobufSchema]
    if schema_type is SchemaType.AVRO:
        try:
            parsed_schema = parse_avro_schema_definition(
                schema_str,
                validate_enum_symbols=validate_avro_enum_symbols,
                validate_names=validate_avro_names,
            )
        except (SchemaParseException, json.JSONDecodeError, TypeError) as e:
            raise InvalidSchema from e

    elif schema_type is SchemaType.JSONSCHEMA:
        try:
            parsed_schema = parse_jsonschema_definition(schema_str)
            # TypeError - Raised when the user forgets to encode the schema as a string.
        except (TypeError, json.JSONDecodeError, SchemaError, AssertionError) as e:
            raise InvalidSchema from e

    elif schema_type is SchemaType.PROTOBUF:
        try:
            parsed_schema = parse_protobuf_schema_definition(schema_str, references, dependencies)
        except (
            TypeError,
            SchemaError,
            AssertionError,
            ProtobufParserRuntimeException,
            IllegalArgumentException,
            IllegalStateException,
            ProtobufError,
            ProtobufException,
            ProtobufSchemaParseException,
        ) as e:
            raise InvalidSchema from e
    else:
        raise InvalidSchema(f"Unknown parser {schema_type} for {schema_str}")

    return ParsedTypedSchema(
        schema_type=schema_type,
        schema_str=schema_str,
        schema=parsed_schema,
        references=references,
        dependencies=dependencies,
    )


class ParsedTypedSchema(TypedSchema):
    """Parsed but unvalidated schema resource.

    This class is used when reading and parsing existing schemas from data store. The intent of this class is to provide
    representation of the schema which can be used to compare existing versions with new version in compatibility check
    and when storing new version.

    This class shall not be used for new schemas received through the public API.

    The intent of this class is not to bypass validation of the syntax of the schema.
    Assumption is that schema is syntactically correct.

    Validations that are bypassed:
     * AVRO: enumeration symbols, namespace and name validity.

    Existing schemas may have been produced with backing schema SDKs that may have passed validation on schemas that
    are considered by the current version of the SDK invalid.
    """

    def __init__(
        self,
        schema_type: SchemaType,
        schema_str: str,
        schema: Union[Draft7Validator, AvroSchema, ProtobufSchema],
        references: Optional[List[Reference]] = None,
        dependencies: Optional[Dict[str, Dependency]] = None,
    ):

        super().__init__(
            schema_type=schema_type, schema_str=schema_str, references=references, dependencies=dependencies, schema=schema
        )

    @staticmethod
    def parse(
        schema_type: SchemaType,
        schema_str: str,
        references: Optional[List[Reference]] = None,
        dependencies: Optional[Dict[str, Dependency]] = None,
    ) -> "ParsedTypedSchema":
        return parse(
            schema_type=schema_type,
            schema_str=schema_str,
            validate_avro_enum_symbols=False,
            validate_avro_names=False,
            references=references,
            dependencies=dependencies,
        )

    def __str__(self) -> str:
        if self.schema_type == SchemaType.PROTOBUF:
            return str(self.schema)
        return super().__str__()


class ValidatedTypedSchema(ParsedTypedSchema):
    """Validated schema resource.

    This class is used when receiving a new schema from through the public API. The intent of this class is to
    provide validation of the schema.
    This class shall not be used when reading and parsing existing schemas.

    The intent of this class is not to validate the syntax of the schema.
    Assumption is that schema is syntactically correct.

    Existing schemas may have been produced with backing schema SDKs that may have passed validation on schemas that
    are considered by the current version of the SDK invalid.
    """

    def __init__(
        self,
        schema_type: SchemaType,
        schema_str: str,
        schema: Union[Draft7Validator, AvroSchema, ProtobufSchema],
        references: Optional[List[Reference]] = None,
        dependencies: Optional[Dict[str, Dependency]] = None,
    ):

        super().__init__(
            schema_type=schema_type, schema_str=schema_str, references=references, dependencies=dependencies, schema=schema
        )

    @staticmethod
    def parse(
        schema_type: SchemaType,
        schema_str: str,
        references: Optional[List[Reference]] = None,
        dependencies: Optional[Dict[str, Dependency]] = None,
    ) -> "ValidatedTypedSchema":
        parsed_schema = parse(
            schema_type=schema_type,
            schema_str=schema_str,
            references=references,
            dependencies=dependencies,
            validate_avro_enum_symbols=True,
            validate_avro_names=True,
        )
        return cast(ValidatedTypedSchema, parsed_schema)
