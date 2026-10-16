package tech.ytsaurus.flow.testutils;

import java.util.ArrayList;
import java.util.List;

import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.Message;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.codec.ByteStringCodec;
import tech.ytsaurus.flow.row.codec.KeyCodec;
import tech.ytsaurus.flow.rpc.TReqProcessBatch;
import tech.ytsaurus.flow.typeinfo.TypeInfo;

public class TestDataUtils {
    public static UnversionedRow createUnversionedRow(TableSchema schema, int stringsSize) {
        var values = new ArrayList<UnversionedValue>(schema.getColumnsCount());
        for (int i = 0; i < schema.getColumnsCount(); ++i) {
            var column = schema.getColumns().get(i);
            switch (column.getType()) {
                case UINT64:
                    values.add(new UnversionedValue(i, ColumnValueType.UINT64, false, 1712182928L));
                    break;
                case INT64:
                    values.add(new UnversionedValue(i, ColumnValueType.INT64, false, 100500L + i));
                    break;
                case STRING:
                    values.add(new UnversionedValue(i, ColumnValueType.STRING, false,
                            RandomStrings.nextAlphanumeric(stringsSize).getBytes()));
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unsupported column type for benchmark: " + column.getType()
                    );
            }
        }
        return new UnversionedRow(values);
    }

    public static List<Message> createMessageList(
            TableSchema messageSchema,
            String streamId,
            int count,
            int stringsSize
    ) {
        var messages = new ArrayList<Message>(count);
        for (int i = 0; i < count; ++i) {
            messages.add(
                    Message.builder()
                            .setStreamId(streamId)
                            .setPayload(new Payload(createUnversionedRow(messageSchema, stringsSize), messageSchema))
                            .build()
            );
        }
        return messages;
    }

    public static List<TReqProcessBatch.TExtendedMessage> createExtenedeMessageList(
            TableSchema payloadSchema,
            TableSchema keySchema,
            int messageCount,
            int keyStringsSize,
            int valueStringsSize,
            KeyCodec keyCodec,
            ByteStringCodec<Payload> payloadCodec
    ) {
        var messages = new ArrayList<TReqProcessBatch.TExtendedMessage>(messageCount);
        for (int i = 0; i < messageCount; ++i) {
            var extMessageBuilder = TReqProcessBatch.TExtendedMessage.newBuilder();
            var messageBuilder = NYT.NFlow.NProto.Message.TMessage.newBuilder();
            messageBuilder.setMessageId("<id>" + i);
            messageBuilder.setStreamSpecId(0L);
            messageBuilder.setSystemTimestamp(1712182928L);
            messageBuilder.setEventTimestamp(1712182927L);
            messageBuilder.setPayload(payloadCodec.encode(
                    new Payload(createUnversionedRow(payloadSchema, valueStringsSize), payloadSchema)
            ));
            extMessageBuilder.setMessage(messageBuilder.build());
            extMessageBuilder.setKey(keyCodec.encode(createUnversionedRow(keySchema, keyStringsSize)));
            messages.add(extMessageBuilder.build());
        }
        return messages;
    }

    public static <T> T createEntity(TypeInfo<T> typeInfo, int stringsSize) {
        T instance = typeInfo.createInstance();
        for (int i = 0; i < typeInfo.getColumns().size(); ++i) {
            var column = typeInfo.getColumns().get(i);
            var schema = column.getSchema();
            switch (schema.getType()) {
                case UINT64:
                    column.set(instance, 1712182928L);
                    break;
                case INT64:
                    column.set(instance, 100500L + i);
                    break;
                case STRING:
                    column.set(instance, RandomStrings.nextAlphanumeric(stringsSize));
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unsupported column type for benchmark: " + schema.getType()
                    );
            }
        }
        return instance;
    }
}
