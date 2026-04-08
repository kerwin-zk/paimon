/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.table.system;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link BranchesTable}. */
class BranchesTableTest extends TableTestBase {
    private static final String tableName = "MyTable";
    private FileStoreTable table;
    private BranchesTable branchesTable;

    @BeforeEach
    void before() throws Exception {
        Identifier identifier = identifier(tableName);
        Schema schema =
                Schema.newBuilder()
                        .column("product_id", DataTypes.INT())
                        .column("price", DataTypes.INT())
                        .column("sales", DataTypes.INT())
                        .primaryKey("product_id")
                        .option("tag.automatic-creation", "watermark")
                        .option("tag.creation-period", "daily")
                        .option("tag.num-retained-max", "3")
                        .build();
        catalog.createTable(identifier, schema, true);
        table = (FileStoreTable) catalog.getTable(identifier);
        TableCommitImpl commit = table.newCommit(commitUser).ignoreEmptyCommit(false);
        commit.commit(
                new ManifestCommittable(
                        0,
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2023-07-18T12:00:01"))
                                .getMillisecond()));
        commit.commit(
                new ManifestCommittable(
                        1,
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2023-07-19T12:00:01"))
                                .getMillisecond()));
        branchesTable = (BranchesTable) catalog.getTable(identifier(tableName + "$branches"));
    }

    @Test
    void testEmptyBranches() throws Exception {
        assertThat(read(branchesTable)).isEmpty();
    }

    @Test
    void testBranches() throws Exception {
        table.createBranch("my_branch1", "2023-07-17");
        table.createBranch("my_branch2", "2023-07-18");
        table.createBranch("my_branch3", "2023-07-18");
        List<InternalRow> branches = read(branchesTable);
        assertThat(branches.size()).isEqualTo(3);
        assertThat(
                        branches.stream()
                                .map(v -> v.getString(0).toString())
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("my_branch1", "my_branch2", "my_branch3");
    }

    @Test
    void testFilterByBranchNameEqual() throws Exception {
        table.createBranch("my_branch1", "2023-07-17");
        table.createBranch("my_branch2", "2023-07-18");
        table.createBranch("my_branch3", "2023-07-18");
        PredicateBuilder builder = new PredicateBuilder(BranchesTable.TABLE_TYPE);
        Predicate predicate = builder.equal(0, BinaryString.fromString("my_branch1"));
        List<InternalRow> result = readWithFilter(branchesTable, predicate);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getString(0).toString()).isEqualTo("my_branch1");
    }

    @Test
    void testFilterByBranchNameEqualNoMatch() throws Exception {
        table.createBranch("my_branch1", "2023-07-17");
        PredicateBuilder builder = new PredicateBuilder(BranchesTable.TABLE_TYPE);
        Predicate predicate = builder.equal(0, BinaryString.fromString("non_existent"));
        List<InternalRow> result = readWithFilter(branchesTable, predicate);
        assertThat(result).isEmpty();
    }

    @Test
    void testFilterByBranchNameIn() throws Exception {
        table.createBranch("my_branch1", "2023-07-17");
        table.createBranch("my_branch2", "2023-07-18");
        table.createBranch("my_branch3", "2023-07-18");
        PredicateBuilder builder = new PredicateBuilder(BranchesTable.TABLE_TYPE);
        Predicate predicate =
                builder.in(
                        0,
                        Arrays.asList(
                                BinaryString.fromString("my_branch1"),
                                BinaryString.fromString("my_branch3")));
        List<InternalRow> result = readWithFilter(branchesTable, predicate);
        assertThat(result.size()).isEqualTo(2);
        assertThat(
                        result.stream()
                                .map(v -> v.getString(0).toString())
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("my_branch1", "my_branch3");
    }

    @Test
    void testFilterByBranchNameInNoMatch() throws Exception {
        table.createBranch("my_branch1", "2023-07-17");
        PredicateBuilder builder = new PredicateBuilder(BranchesTable.TABLE_TYPE);
        Predicate predicate =
                builder.in(
                        0,
                        Arrays.asList(
                                BinaryString.fromString("non_existent1"),
                                BinaryString.fromString("non_existent2")));
        List<InternalRow> result = readWithFilter(branchesTable, predicate);
        assertThat(result).isEmpty();
    }

    private List<InternalRow> readWithFilter(Table table, Predicate predicate) throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder().withFilter(predicate);
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        InternalRowSerializer serializer = new InternalRowSerializer(table.rowType());
        List<InternalRow> rows = new ArrayList<>();
        reader.forEachRemaining(row -> rows.add(serializer.copy(row)));
        return rows;
    }
}
