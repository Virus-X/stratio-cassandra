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
package com.stratio.cassandra.index.schema;

import com.stratio.cassandra.index.AnalyzerFactory;
import org.apache.cassandra.db.marshal.*;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Arrays;

/**
 * A {@link ColumnMapper} to map a string, tokenized field.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ColumnMapperText extends ColumnMapper<String>
{

    /** The Lucene {@link Analyzer} to be used. */
    private Analyzer analyzer;

    /**
     * Builds a new {@link ColumnMapperText} using the specified Lucene {@link Analyzer}.
     * @param analyzerClassName The Lucene {@link Analyzer} to be used.
     */
    @JsonCreator
    public ColumnMapperText(@JsonProperty("analyzer") String analyzerClassName)
    {
        super(new AbstractType<?>[]{
                AsciiType.instance,
                UTF8Type.instance,
                Int32Type.instance,
                LongType.instance,
                IntegerType.instance,
                FloatType.instance,
                DoubleType.instance,
                BooleanType.instance,
                UUIDType.instance,
                TimeUUIDType.instance,
                TimestampType.instance,
                BytesType.instance,
                InetAddressType.instance}, new AbstractType[]{});
        if (analyzerClassName != null)
        {
            this.analyzer = AnalyzerFactory.getAnalyzer(analyzerClassName);
        }
        else
        {
            this.analyzer = Schema.DEFAULT_ANALYZER;
        }
    }

    /** {@inheritDoc} */
    @Override
    public Analyzer analyzer()
    {
        return analyzer;
    }

    /** {@inheritDoc} */
    public String indexValue(String name, Object value)
    {
        if (value == null)
        {
            return null;
        }
        else
        {
            return value.toString();
        }
    }

    /** {@inheritDoc} */
    @Override
    public String queryValue(String name, Object value)
    {
        return indexValue(name, value);
    }

    /** {@inheritDoc} */
    @Override
    public Iterable<Field> fields(String name, Object value)
    {
        String text = indexValue(name, value);
        return Arrays.asList((Field)new TextField(name, text, STORE));
    }

    /** {@inheritDoc} */
    @Override
    public SortField sortField(String field, boolean reverse)
    {
        return new SortField(field, Type.STRING, reverse);
    }

    /** {@inheritDoc} */
    @Override
    public Class<String> baseClass()
    {
        return String.class;
    }

    /** {@inheritDoc} */
    @Override
    public String toString()
    {
        return new ToStringBuilder(this).append("analyzer", analyzer).toString();
    }
}
