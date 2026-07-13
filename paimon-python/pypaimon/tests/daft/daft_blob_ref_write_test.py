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
"""Tests for pypaimon.daft.blob_ref: building a reference-blob column (serialized
BlobDescriptor) so write_paimon can register external files as blobs without the
client holding the blob bytes."""
import os
import shutil
import tempfile
import unittest

import pyarrow as pa
import pytest

pypaimon = pytest.importorskip("pypaimon")
daft = pytest.importorskip("daft")

from daft import col

from pypaimon import CatalogFactory, Schema
from pypaimon.daft import blob_ref, read_blob, read_paimon, write_paimon
from pypaimon.daft.daft_compat import has_file_range_reads
from pypaimon.table.row.blob import BlobDescriptor


class BlobRefExpressionTest(unittest.TestCase):
    """Pure expression behavior; runs on any installed Daft."""

    def test_builds_serialized_descriptors(self):
        df = daft.from_pydict({"uri": ["file:///a/x.bin", "file:///a/y.bin"]})
        out = df.select(blob_ref(col("uri")).alias("d")).to_pydict()["d"]
        descs = [BlobDescriptor.deserialize(bytes(b)) for b in out]
        self.assertEqual([d.uri for d in descs], ["file:///a/x.bin", "file:///a/y.bin"])
        self.assertEqual([d.offset for d in descs], [0, 0])
        self.assertEqual([d.length for d in descs], [-1, -1])
        self.assertTrue(all(BlobDescriptor.is_blob_descriptor(bytes(b)) for b in out))

    def test_offset_and_length_applied(self):
        df = daft.from_pydict({"uri": ["file:///a/x.bin"]})
        out = df.select(blob_ref(col("uri"), offset=8, length=16).alias("d")).to_pydict()["d"]
        d = BlobDescriptor.deserialize(bytes(out[0]))
        self.assertEqual((d.offset, d.length), (8, 16))

    def test_accepts_column_name_string(self):
        df = daft.from_pydict({"uri": ["file:///a/x.bin"]})
        out = df.select(blob_ref("uri").alias("d")).to_pydict()["d"]
        self.assertEqual(BlobDescriptor.deserialize(bytes(out[0])).uri, "file:///a/x.bin")

    def test_null_uri_yields_null(self):
        df = daft.from_pydict({"uri": ["file:///a/x.bin", None]})
        out = df.select(blob_ref(col("uri")).alias("d")).to_pydict()["d"]
        self.assertIsNotNone(out[0])
        self.assertIsNone(out[1])


@unittest.skipUnless(has_file_range_reads(), "installed Daft lacks File range reads")
class BlobRefWritePaimonTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.catalog_options = {"warehouse": os.path.join(self.tempdir, "wh")}
        self.catalog = CatalogFactory.create(self.catalog_options)
        self.catalog.create_database("default", True)
        self.table = "default.blob_ref_write"
        self.pa_schema = pa.schema([("id", pa.int32()), ("content", pa.large_binary())])
        self.catalog.create_table(self.table, Schema.from_pyarrow_schema(
            self.pa_schema,
            options={"row-tracking.enabled": "true", "data-evolution.enabled": "true"}),
            False)
        self.payloads = []
        self.uris = []
        for i in range(4):
            p = os.path.join(self.tempdir, f"ext_{i}.bin")
            payload = os.urandom(128)
            with open(p, "wb") as f:
                f.write(payload)
            self.payloads.append(payload)
            self.uris.append("file://" + p)

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_register_external_files_by_reference(self):
        df = daft.from_pydict({"id": list(range(4)), "uri": self.uris})
        df = df.with_column("content", blob_ref(col("uri"))).select(col("id"), col("content"))
        write_paimon(df, self.table, self.catalog_options, mode="append")

        out = (read_paimon(self.table, self.catalog_options)
               .select(col("id"), read_blob(col("content"), self.catalog_options, self.table)
                       .alias("b")).to_pylist())
        got = {r["id"]: r["b"] for r in out}
        self.assertEqual(len(out), 4)
        for i in range(4):
            self.assertEqual(got[i], self.payloads[i])

    def test_reference_subrange(self):
        # A descriptor with offset/length registers only a slice of the file.
        df = daft.from_pydict({"id": [0], "uri": [self.uris[0]]})
        df = df.with_column("content", blob_ref(col("uri"), offset=10, length=20)).select(
            col("id"), col("content"))
        write_paimon(df, self.table, self.catalog_options, mode="append")

        out = (read_paimon(self.table, self.catalog_options)
               .select(read_blob(col("content"), self.catalog_options, self.table).alias("b"))
               .to_pylist())
        self.assertIn(self.payloads[0][10:30], [r["b"] for r in out])


if __name__ == "__main__":
    unittest.main()
