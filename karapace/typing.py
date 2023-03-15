"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from typing import Any, Dict, Union

JsonData = Any  # Data that will be encoded to or has been parsed from JSON

Subject = str

Version = Union[int, str]
ResolvedVersion = int

Schema = Dict[str, Any]
SchemaId = int
