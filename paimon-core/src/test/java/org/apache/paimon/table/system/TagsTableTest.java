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
import org.apache.paimon.data.GenericRow;
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
import org.apache.paimon.tag.Tag;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.TagManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link TagsTable}. */
class TagsTableTest extends TableTestBase {

    private static final String tableName = "MyTable";
    private TagsTable tagsTable;
    private TagManager tagManager;
    private FileStoreTable table;

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
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        this.table = table;
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
        tagsTable = (TagsTable) catalog.getTable(identifier(tableName + "$tags"));
        tagManager = table.store().newTagManager();
        table.createTag("many-tags-test");
    }

    @Test
    void testTagsTable() throws Exception {
        List<InternalRow> expectRow = getExpectedResult();
        List<InternalRow> result = read(tagsTable);
        assertThat(result).containsExactlyElementsOf(expectRow);
    }

    private List<InternalRow> getExpectedResult() {
        Map<String, InternalRow> tagToRows = new TreeMap<>();
        for (Pair<Tag, String> snapshot : tagManager.tagObjects()) {
            Tag tag = snapshot.getKey();
            String tagName = snapshot.getValue();
            tagToRows.put(
                    tagName,
                    GenericRow.of(
                            BinaryString.fromString(tagName),
                            tag.id(),
                            tag.schemaId(),
                            Timestamp.fromLocalDateTime(
                                    DateTimeUtils.toLocalDateTime(tag.timeMillis())),
                            tag.totalRecordCount(),
                            tag.getTagCreateTime() == null
                                    ? null
                                    : Timestamp.fromLocalDateTime(tag.getTagCreateTime()),
                            tag.getTagTimeRetained() == null
                                    ? null
                                    : BinaryString.fromString(
                                            tag.getTagTimeRetained().toString())));
        }

        List<InternalRow> internalRows = new ArrayList<>();
        for (Map.Entry<String, InternalRow> entry : tagToRows.entrySet()) {
            internalRows.add(entry.getValue());
        }
        return internalRows;
    }

    @Test
    void testFilterByTagNameEqual() throws Exception {
        table.createTag("test-tag-2");
        table.createTag("test-tag-3");
        PredicateBuilder builder = new PredicateBuilder(TagsTable.TABLE_TYPE);
        Predicate predicate = builder.equal(0, BinaryString.fromString("many-tags-test"));
        List<InternalRow> result = readWithFilter(tagsTable, predicate);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getString(0).toString()).isEqualTo("many-tags-test");
    }

    @Test
    void testFilterByTagNameEqualNoMatch() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(TagsTable.TABLE_TYPE);
        Predicate predicate = builder.equal(0, BinaryString.fromString("non_existent"));
        List<InternalRow> result = readWithFilter(tagsTable, predicate);
        assertThat(result).isEmpty();
    }

    @Test
    void testFilterByTagNameIn() throws Exception {
        table.createTag("test-tag-2");
        table.createTag("test-tag-3");
        PredicateBuilder builder = new PredicateBuilder(TagsTable.TABLE_TYPE);
        Predicate predicate =
                builder.in(
                        0,
                        Arrays.asList(
                                BinaryString.fromString("many-tags-test"),
                                BinaryString.fromString("test-tag-2")));
        List<InternalRow> result = readWithFilter(tagsTable, predicate);
        assertThat(result.size()).isEqualTo(2);
        assertThat(
                        result.stream()
                                .map(v -> v.getString(0).toString())
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("many-tags-test", "test-tag-2");
    }

    @Test
    void testFilterByTagNameInNoMatch() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(TagsTable.TABLE_TYPE);
        Predicate predicate =
                builder.in(
                        0,
                        Arrays.asList(
                                BinaryString.fromString("non_existent1"),
                                BinaryString.fromString("non_existent2")));
        List<InternalRow> result = readWithFilter(tagsTable, predicate);
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
