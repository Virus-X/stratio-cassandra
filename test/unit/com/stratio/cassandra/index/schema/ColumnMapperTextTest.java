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

import java.io.IOException;
import java.util.UUID;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.junit.Assert;
import org.junit.Test;

public class ColumnMapperTextTest
{

    @Test()
    public void testAnalyzerNull()
    {
        ColumnMapperText mapper = new ColumnMapperText(null);
        String parsed = mapper.indexValue("test", null);
        Assert.assertNull(parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAnalyzerInvalid()
    {
        ColumnMapperText mapper = new ColumnMapperText("hello");
        String parsed = mapper.indexValue("test", null);
        Assert.assertNull(parsed);
    }

    @Test()
    public void testValueNull()
    {
        ColumnMapperText mapper = new ColumnMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.indexValue("test", null);
        Assert.assertNull(parsed);
    }

    @Test
    public void testValueInteger()
    {
        ColumnMapperText mapper = new ColumnMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.indexValue("test", 3);
        Assert.assertEquals("3", parsed);
    }

    @Test
    public void testValueLong()
    {
        ColumnMapperText mapper = new ColumnMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.indexValue("test", 3l);
        Assert.assertEquals("3", parsed);
    }

    @Test
    public void testValueFloatWithoutDecimal()
    {
        ColumnMapperText mapper = new ColumnMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.indexValue("test", 3f);
        Assert.assertEquals("3.0", parsed);
    }

    @Test
    public void testValueFloatWithDecimalFloor()
    {
        ColumnMapperText mapper = new ColumnMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.indexValue("test", 3.5f);
        Assert.assertEquals("3.5", parsed);

    }

    @Test
    public void testValueFloatWithDecimalCeil()
    {
        ColumnMapperText mapper = new ColumnMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.indexValue("test", 3.6f);
        Assert.assertEquals("3.6", parsed);
    }

    @Test
    public void testValueDoubleWithoutDecimal()
    {
        ColumnMapperText mapper = new ColumnMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.indexValue("test", 3d);
        Assert.assertEquals("3.0", parsed);
    }

    @Test
    public void testValueDoubleWithDecimalFloor()
    {
        ColumnMapperText mapper = new ColumnMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.indexValue("test", 3.5d);
        Assert.assertEquals("3.5", parsed);

    }

    @Test
    public void testValueDoubleWithDecimalCeil()
    {
        ColumnMapperText mapper = new ColumnMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.indexValue("test", 3.6d);
        Assert.assertEquals("3.6", parsed);

    }

    @Test
    public void testValueStringWithoutDecimal()
    {
        ColumnMapperText mapper = new ColumnMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.indexValue("test", "3");
        Assert.assertEquals("3", parsed);
    }

    @Test
    public void testValueStringWithDecimalFloor()
    {
        ColumnMapperText mapper = new ColumnMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.indexValue("test", "3.2");
        Assert.assertEquals("3.2", parsed);
    }

    @Test
    public void testValueStringWithDecimalCeil()
    {
        ColumnMapperText mapper = new ColumnMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.indexValue("test", "3.6");
        Assert.assertEquals("3.6", parsed);

    }

    @Test
    public void testValueUUID()
    {
        ColumnMapperText mapper = new ColumnMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
        String parsed = mapper.indexValue("test", UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
        Assert.assertEquals("550e8400-e29b-41d4-a716-446655440000", parsed);
    }

    @Test
    public void testField()
    {
        ColumnMapperText mapper = new ColumnMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
        Field field = mapper.fields("name", "hello").iterator().next();
        Assert.assertNotNull(field);
        Assert.assertEquals("hello", field.stringValue());
        Assert.assertEquals("name", field.name());
        Assert.assertEquals(false, field.fieldType().stored());
    }

    @Test
    public void testExtractAnalyzers()
    {
        ColumnMapperText mapper = new ColumnMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
        Analyzer analyzer = mapper.analyzer();
        Assert.assertEquals(org.apache.lucene.analysis.en.EnglishAnalyzer.class, analyzer.getClass());
    }

    @Test
    public void testParseJSONWithAnayzer() throws IOException
    {
        String json = "{fields:{age:{type:\"text\", analyzer:\"org.apache.lucene.analysis.en.EnglishAnalyzer\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper<?> columnMapper = schema.getMapper("age");
        Assert.assertNotNull(columnMapper);
        Assert.assertEquals(ColumnMapperText.class, columnMapper.getClass());
    }

    @Test
    public void testParseJSONWithoutAnalyzer() throws IOException
    {
        String json = "{fields:{age:{type:\"text\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper<?> columnMapper = schema.getMapper("age");
        Assert.assertNotNull(columnMapper);
        Assert.assertEquals(ColumnMapperText.class, columnMapper.getClass());
    }

    @Test
    public void testParseJSONEmpty() throws IOException
    {
        String json = "{fields:{}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper<?> columnMapper = schema.getMapper("age");
        Assert.assertNull(columnMapper);
    }

    @Test(expected = IOException.class)
    public void testParseJSONInvalid() throws IOException
    {
        String json = "{fields:{age:{}}";
        Schema.fromJson(json);
    }
}
