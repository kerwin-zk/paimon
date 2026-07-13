################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""Utilities for deserializing Paimon BlobDescriptor bytes into FileReference arrays."""

from __future__ import annotations

import struct

import pyarrow as pa

from pypaimon.daft.daft_compat import file_range_position_field, file_range_size_field
from pypaimon.table.row.blob import BlobDescriptor


FILE_PHYSICAL_TYPE = pa.struct(
    [
        pa.field("url", pa.large_utf8()),
        pa.field("io_config", pa.large_binary()),
        pa.field(file_range_position_field(), pa.int64()),
        pa.field(file_range_size_field(), pa.int64()),
    ]
)


def _deserialize_one(data: bytes) -> tuple[str, int, int]:
    """Deserialize a single BlobDescriptor -> (url, offset, length)."""
    try:
        descriptor = BlobDescriptor.deserialize(data)
    except (struct.error, UnicodeDecodeError) as e:
        raise ValueError("Invalid BlobDescriptor data") from e
    return descriptor.uri, descriptor.offset, descriptor.length


def blob_ref(uri, offset: int = 0, length: int = -1):
    """Build a reference-blob column from a URI column for ``write_paimon``.

    Returns a Daft expression that serializes each URI into a ``BlobDescriptor``
    (the wire form ``write_paimon`` recognizes: on write, Paimon resolves the URI
    and streams the external bytes into its own ``.blob`` storage, so the client
    never materializes the blob payload). This is the write-side counterpart of
    ``read_blob`` / ``open_blob``.

    ``uri`` is a Daft expression or a column name. ``offset`` / ``length`` are
    scalars applied to every row (default: the whole file, ``offset=0``,
    ``length=-1``). Null URIs produce null cells.
    """
    import daft
    from daft import DataType

    if isinstance(uri, str):
        uri = daft.col(uri)

    @daft.func.batch(return_dtype=DataType.binary())
    def _to_descriptors(uris):
        return [
            None if u is None else BlobDescriptor(u, offset, length).serialize()
            for u in uris.to_arrow().to_pylist()
        ]

    return _to_descriptors(uri)


def blob_column_to_file_array(column: pa.Array, io_config_bytes: bytes | None = None) -> pa.Array:
    """Convert a large_binary column of serialized BlobDescriptors to a File-compatible struct.

    ``io_config_bytes`` (serialized IOConfig) is embedded into each File so native File ops carry
    credentials; when None the io_config is left null and ops fall back to Daft's global IOConfig.
    """
    urls: list[str | None] = []
    offsets: list[int | None] = []
    lengths: list[int | None] = []

    for value in column:
        if value is None or not value.is_valid:
            urls.append(None)
            offsets.append(None)
            lengths.append(None)
        else:
            raw = value.as_py()
            uri, off, length = _deserialize_one(raw)
            urls.append(uri)
            offsets.append(off)
            lengths.append(length)

    n = len(urls)
    if io_config_bytes is None:
        io_configs: pa.Array = pa.nulls(n, type=pa.large_binary())
    else:
        # Only populate io_config for valid rows; keep null rows null.
        io_configs = pa.array(
            [io_config_bytes if u is not None else None for u in urls],
            type=pa.large_binary(),
        )
    return pa.StructArray.from_arrays(
        [
            pa.array(urls, type=pa.large_utf8()),
            io_configs,
            pa.array(offsets, type=pa.int64()),
            pa.array(lengths, type=pa.int64()),
        ],
        names=["url", "io_config", file_range_position_field(), file_range_size_field()],
    )
