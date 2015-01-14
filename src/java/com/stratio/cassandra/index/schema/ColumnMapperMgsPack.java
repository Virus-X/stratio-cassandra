package com.stratio.cassandra.index.schema;

import com.stratio.cassandra.index.util.ByteBufferUtils;
import org.apache.cassandra.cql3.Constants;
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
import org.codehaus.jackson.annotate.JsonProperty;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.MapCursor;
import org.msgpack.value.ValueRef;
import org.msgpack.value.holder.ValueHolder;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by vgoncharenko on 13.01.2015.
 */
public class ColumnMapperMgsPack extends ColumnMapper<String> {

    /** The default limit for depth of message pack fields. */
    private static final int DEFAULT_DEPTH_LIMIT = 2;

    /** The maximum depth of object field */
    private final int fieldDepthLimit;

    /**
     * Builds a new {@link ColumnMapperBlob}.
     */
    @JsonCreator
    public ColumnMapperMgsPack(@JsonProperty("depthLimit") Integer depthLimit) {
        super(new AbstractType<?>[]{AsciiType.instance, UTF8Type.instance, BytesType.instance}, new AbstractType[]{});
        fieldDepthLimit = depthLimit == null ? DEFAULT_DEPTH_LIMIT : depthLimit;
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
            unpacker.unpackValue(v);
            return extractFields(name, v.getRef(), 0);
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

    private List<Field> extractFields(String name, ValueRef v, int depth){
        ArrayList<Field> fields = new ArrayList<Field>();
        switch (v.getValueType()) {
            case NIL:
            case EXTENDED:
                // NOT Supported
                break;
            case BOOLEAN:
                fields.add(new StringField(name, v.asBoolean().toBoolean() ? "true" : "false", STORE));
                break;
            case INTEGER:
                fields.add(new LongField(name, v.asNumber().asLong(), STORE));
                break;
            case FLOAT:
                fields.add(new DoubleField(name, v.asFloat().toDouble(), STORE));
                break;
            case STRING:
                fields.add(new StringField(name, v.asString().toString(), STORE));
                break;
            case BINARY:
                fields.add(new StringField(name, Hex.bytesToHex(v.asBinary().toMessageBuffer().getArray()), STORE));
                break;
            case ARRAY:
                for (ValueRef val : v.getArrayCursor())                {
                    fields.addAll(extractFields(name,val, depth));
                }
                break;
            case MAP:
                if (depth >= fieldDepthLimit){
                    // Depth limit reached
                    break;
                }

                MapCursor cursor = v.getMapCursor();
                while(cursor.hasNext()){
                    ValueRef key = cursor.nextKeyOrValue();
                    ValueRef val = cursor.nextKeyOrValue();

                    if (key.isNumber() || key.isBoolean() || key.isString()){
                        String keyName = name + "." + key.toString();
                        fields.addAll(extractFields(keyName,val, depth + 1));
                    }else{
                        // Key cannot have type map or array
                        continue;
                    }
                }
                break;
        }

        return fields;
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
