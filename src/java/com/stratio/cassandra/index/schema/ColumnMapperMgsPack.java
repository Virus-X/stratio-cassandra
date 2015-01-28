package com.stratio.cassandra.index.schema;

import com.stratio.cassandra.index.util.ByteBufferUtils;
import com.stratio.cassandra.index.util.Log;
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
import org.msgpack.value.ValueType;
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

    /** The allows case insensitive searches in string fields */
    private final boolean caseInsensitiveStrings;

    /**
     * Builds a new {@link ColumnMapperBlob}.
     */
    @JsonCreator
    public ColumnMapperMgsPack(
            @JsonProperty("depth_limit") Integer depthLimit,
            @JsonProperty("case_insensitive") Boolean caseInsensitiveStrings) {

        super(new AbstractType<?>[]{AsciiType.instance, UTF8Type.instance, BytesType.instance}, new AbstractType[]{});
        fieldDepthLimit = depthLimit == null ? DEFAULT_DEPTH_LIMIT : depthLimit;
        this.caseInsensitiveStrings = caseInsensitiveStrings != null && caseInsensitiveStrings;
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
        ValueRef unpacked = Unpack(value);

        if (unpacked == null) {
            return new ArrayList<Field>();
        }

        return extractFields(name, unpacked, 0);
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

    @Override
    public int Compare(Column col1, Column col2){
        if (col1 == null)
        {
            return col2 == null ? 0 : 1;
        }
        if (col2 == null)
        {
            return -1;
        }

        ValueRef val1 = Unpack(col1.getValue());
        ValueRef val2 = Unpack(col2.getValue());
        return CompareValues(val1,val2);
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
            case FLOAT:
                fields.add(new DoubleField(name, v.asNumber().toDouble(), STORE));
                break;
            case STRING:
                String data = caseInsensitiveStrings
                        ? v.asString().toString().toLowerCase()
                        : v.asString().toString();

                fields.add(new StringField(name, data, STORE));
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

    private int CompareValues(ValueRef a, ValueRef b){
        if (a == null || a.isNil())
        {
            return b == null || b.isNil() ? 0 : -1;
        }
        if (b == null || b.isNil())
        {
            return 1;
        }

        Log.info("comparing " + a + " & " + b);

       switch (a.getValueType())
       {
           case INTEGER:
           case FLOAT:
               if (b.getValueType() == ValueType.INTEGER || b.getValueType() == ValueType.FLOAT){
                   return Double.compare(a.asNumber().toDouble(), b.asNumber().toDouble());
               }else {
                   // Assume numbers are always < than other types
                    return -1;
               }
           case BOOLEAN:
           case STRING:
               if (b.getValueType() == ValueType.INTEGER || b.getValueType() == ValueType.FLOAT){
                   return 1;
               }else if (b.getValueType() == ValueType.STRING || b.getValueType() == ValueType.BOOLEAN){
                   String stringA = a.getValueType() == ValueType.STRING ? a.asString().toString() : a.asBoolean().toString();
                   String stringB = b.getValueType() == ValueType.STRING ? b.asString().toString() : b.asBoolean().toString();

                   return this.caseInsensitiveStrings
                           ? stringA.compareToIgnoreCase(stringB)
                           : stringA.compareTo(stringB);

               }else{
                   // Assume strings are always < arrays, maps, and complex objects.
                   return -1;
               }
           default:
               if (b.getValueType() == ValueType.INTEGER ||
                       b.getValueType() == ValueType.FLOAT ||
                       b.getValueType() == ValueType.STRING ||
                       b.getValueType() == ValueType.BOOLEAN)
               {
                   return 1;
               }else{
                   return 0;
               }
       }
    }

    private ValueRef Unpack(Object value){
        byte [] data = asBytes(value);
        try {
            ValueHolder v = new ValueHolder();
            MessagePack.newDefaultUnpacker(data).unpackValue(v);
            return v.getRef();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
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
