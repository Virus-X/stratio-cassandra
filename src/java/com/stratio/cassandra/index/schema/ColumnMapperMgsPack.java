package com.stratio.cassandra.index.schema;

import com.stratio.cassandra.index.util.ByteBufferUtils;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.codehaus.jackson.annotate.JsonCreator;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.holder.ValueHolder;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by vgoncharenko on 13.01.2015.
 */
public class ColumnMapperMgsPack extends ColumnMapper<String> {
    /**
     * Builds a new {@link ColumnMapperBlob}.
     */
    @JsonCreator
    public ColumnMapperMgsPack() {
        super(new AbstractType<?>[]{AsciiType.instance, UTF8Type.instance, BytesType.instance}, new AbstractType[]{});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Analyzer analyzer() {
        return EMPTY_ANALYZER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String queryValue(String name, Object value) {
        if (value == null) {
            return null;
        } else {
            return value.toString().toLowerCase().replaceFirst("0x", "");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterable<Field> fields(String name, Object value) {
        byte[] bytes = asBytes(value);

        if (bytes == null) {
            return new ArrayList<Field>();
        }

        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes);
        try {
            ValueHolder v = new ValueHolder();
            MessageFormat format = unpacker.unpackValue(v);
            switch (format.getValueType()) {
                case NIL:
                    return new ArrayList<Field>();
                case BOOLEAN:
                    return Arrays.asList((Field)new StringField(name, v.get().asBoolean().toBoolean() ? "true" : "false", STORE));
                case INTEGER:
                    return Arrays.asList((Field)new LongField(name, v.getIntegerHolder().asLong(), STORE));
                case FLOAT:
                    return Arrays.asList((Field)new DoubleField(name, v.getFloatHolder().toDouble(), STORE));
                case STRING:
                    return Arrays.asList((Field)new StringField(name, v.get().asString().toString(), STORE));
                case BINARY:
                    return Arrays.asList((Field)new StringField(name, Hex.bytesToHex(v.get().asBinary().toMessageBuffer().getArray()), STORE));
                case ARRAY:
                case MAP:
                case EXTENDED:
                    // NOT Supported yet
                    return new ArrayList<Field>();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return new ArrayList<Field>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SortField sortField(String field, boolean reverse) {
        return new SortField(field, Type.STRING, reverse);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<String> baseClass() {
        return String.class;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this).toString();
    }

    private byte[] asBytes(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof ByteBuffer) {
            ByteBuffer bb = (ByteBuffer) value;
            return ByteBufferUtils.asArray(bb);
        } else if (value instanceof byte[]) {
            return (byte[]) value;
        } else if (value instanceof String) {
            String string = (String) value;
            string = string.replaceFirst("0x", "");
            return Hex.hexToBytes(string);
        } else {
            throw new IllegalArgumentException(String.format("Value '%s' cannot be cast to byte array", value));
        }
    }
}
