/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.index;

import com.stratio.cassandra.index.query.Sort;
import com.stratio.cassandra.index.query.SortField;
import com.stratio.cassandra.index.schema.Columns;
import com.stratio.cassandra.index.schema.Schema;
import com.stratio.cassandra.index.util.ComparatorChain;
import org.apache.cassandra.db.Row;

import java.util.Comparator;

/**
 * A {@link Comparator} for comparing {@link Row}s according to a certain {@link com.stratio.cassandra.index.query.Sort}.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class RowComparatorSorting implements RowComparator
{
    private final RowMapper rowMapper;
    private final ComparatorChain<Columns> comparatorChain;

    /**
     * @param rowMapper The indexing {@link Schema} of the {@link Row}s to be compared.
     * @param sort      The {@link com.stratio.cassandra.index.query.Sort} inf which the {@link Row} comparison is
     *                  based.
     * @param schema The {@link Schema} to be used.
     */
    public RowComparatorSorting(RowMapper rowMapper, Sort sort, Schema schema)
    {
        this.rowMapper = rowMapper;
        comparatorChain = new ComparatorChain<>();
        for (SortField sortField : sort.getSortFields())
        {
            Comparator<Columns> comparator = sortField.comparator(schema);
            comparatorChain.addComparator(comparator);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @param row1 A {@link Row}.
     * @param row2 A {@link Row}.
     * @return A negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater
     * than the second according to a {@link com.stratio.cassandra.index.query.Sort}.
     */
    @Override
    public int compare(Row row1, Row row2)
    {
        Columns columns1 = rowMapper.columns(row1);
        Columns columns2 = rowMapper.columns(row2);
        return comparatorChain.compare(columns1, columns2);
    }
}
