package cn.hit.gd.api;

import java.util.Arrays;

import static cn.hit.gd.api.exception.ErrorExpressionContant.STREAM_FIELDS_OUT_BOUND;

/**
 * @author: create by ywrao
 * @version: v1.0
 * @description: cn.hit.gd.api
 * @date:2019/1/30
 **/
public class StreamFields {
    private final int size;
    private final String[] names;
    private final StreamFieldType[] types;

    public StreamFields(int size) {
        this.size = size;
        names = new String[size];
        types = new StreamFieldType[size];
    }

    public void setField(int index, String name, StreamFieldType type) {
        if (index < 0 || index > size) {
            throw new RuntimeException(STREAM_FIELDS_OUT_BOUND + " " + index);
        }
        names[index] = name;
        types[index] = type;
    }

    public String getFieldName(int index) {
        if (index < 0 || index > size) {
            throw new RuntimeException(STREAM_FIELDS_OUT_BOUND + " " + index);
        }
        return names[index];
    }

    public StreamFieldType getStreamFieldType(int index) {
        if (index < 0 || index > size) {
            throw new RuntimeException(STREAM_FIELDS_OUT_BOUND + " " + index);
        }
        return types[index];
    }

    public int getSize() {
        return size;
    }

    public String[] getNames() {
        return names;
    }

    public StreamFieldType[] getTypes() {
        return types;
    }

    @Override
    public String toString() {
        return "StreamFields{" +
                "size=" + size +
                ", names=" + Arrays.toString(names) +
                ", types=" + Arrays.toString(types) +
                '}';
    }
}
