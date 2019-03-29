package cn.hit.gd.api.util;

import cn.hit.gd.api.StreamFieldType;
import cn.hit.gd.api.StreamFields;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;

import java.util.LinkedList;
import java.util.List;

import static cn.hit.gd.api.exception.ErrorExpressionContant.UNKNOWN_FIELD_TYPE;


/**
 * @author: create by ywrao
 * @version: v1.0
 * @description: cn.hit.gd.api.util
 * @date:2019/1/30
 **/
public class FieldsUtil {

    public static String getFieldsName(RowTypeInfo rowTypeInfo) {
        if (rowTypeInfo != null) {
            String[] fliedsName = rowTypeInfo.getFieldNames();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < fliedsName.length; i++) {
                sb.append(fliedsName[i].trim());
                sb.append(",");
            }
            return sb.toString();
        } else throw new RuntimeException();
    }

    public static TypeInformation[] changeTimeIndicatoeTypeInfo(TypeInformation[] typeInformations) {
        List<TypeInformation> typeInformationList = new LinkedList<>();
        for (TypeInformation typeInformation : typeInformations) {
            typeInformationList.add(typeInformation instanceof TimeIndicatorTypeInfo ? SqlTimeTypeInfo.TIMESTAMP : typeInformation);
        }
        return typeInformationList.toArray(new TypeInformation[typeInformations.length]);
    }

    public static StreamFields rowTypeInfo2StreamFields(RowTypeInfo rowTypeInfo) {
        String[] names = rowTypeInfo.getFieldNames();
        TypeInformation[] typeInformations = rowTypeInfo.getFieldTypes();
        StreamFields streamFields = new StreamFields(names.length);

        for (int i = 0; i < names.length; i++) {
            TypeInformation typeInformation = typeInformations[i];
            if (BasicTypeInfo.INT_TYPE_INFO.equals(typeInformation)) {
                streamFields.setField(i, names[i], StreamFieldType.INTEGER);
            } else if (BasicTypeInfo.BIG_DEC_TYPE_INFO.equals(typeInformation)) {
                streamFields.setField(i, names[i], StreamFieldType.DECIMAL);
            } else if (BasicTypeInfo.LONG_TYPE_INFO.equals(typeInformation)) {
                streamFields.setField(i, names[i], StreamFieldType.BIGINT);
            } else if (BasicTypeInfo.SHORT_TYPE_INFO.equals(typeInformation)) {
                streamFields.setField(i, names[i], StreamFieldType.SMALLINT);
            } else if (BasicTypeInfo.STRING_TYPE_INFO.equals(typeInformation)) {
                streamFields.setField(i, names[i], StreamFieldType.CHAR);
            } else if (SqlTimeTypeInfo.TIMESTAMP.equals(typeInformation)) {
                streamFields.setField(i, names[i], StreamFieldType.TIMESTAMP);
            } else {
                throw new RuntimeException();
            }
        }
        return streamFields;
    }

    public static RowTypeInfo tableSchema2RowTypeInfo(TableSchema tableSchema) {
        return new RowTypeInfo(tableSchema.getFieldTypes(), tableSchema.getFieldNames());
    }

    public static RowTypeInfo streamFields2RowTypeInfo(StreamFields streamFields) {
        String[] names = new String[streamFields.getSize()];
        TypeInformation[] typeInformations = new TypeInformation[streamFields.getSize()];

        for (int i = 0; i < streamFields.getSize(); i++) {
            names[i] = streamFields.getFieldName(i);
            StreamFieldType type = streamFields.getStreamFieldType(i);
            switch (type) {
                case CHAR: {
                    typeInformations[i] = BasicTypeInfo.STRING_TYPE_INFO;
                    break;
                }
                case BIGINT: {
                    typeInformations[i] = BasicTypeInfo.LONG_TYPE_INFO;
                    break;
                }
                case DECIMAL: {
                    typeInformations[i] = BasicTypeInfo.BIG_DEC_TYPE_INFO;
                    break;
                }
                case INTEGER: {
                    typeInformations[i] = BasicTypeInfo.INT_TYPE_INFO;
                    break;
                }
                case SMALLINT: {
                    typeInformations[i] = BasicTypeInfo.SHORT_TYPE_INFO;
                    break;
                }
                case TIMESTAMP: {
                    typeInformations[i] = SqlTimeTypeInfo.TIMESTAMP;
                    break;
                }
                default:
                    throw new RuntimeException(UNKNOWN_FIELD_TYPE + type);
            }
        }
        return new RowTypeInfo(typeInformations, names);
    }
}
