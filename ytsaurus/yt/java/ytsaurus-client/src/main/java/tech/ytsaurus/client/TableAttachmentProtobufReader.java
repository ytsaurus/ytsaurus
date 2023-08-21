package tech.ytsaurus.client;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Message;

import static tech.ytsaurus.core.utils.ClassUtils.castToType;

public class TableAttachmentProtobufReader<T extends Message> extends TableAttachmentRowsetReader<T> {
    private final Message.Builder messageBuilder;

    public TableAttachmentProtobufReader(Message.Builder messageBuilder) {
        this.messageBuilder = messageBuilder;
    }

    @Override
    protected List<T> parseMergedRow(ByteBuffer buffer, int size) {
        byte[] data = new byte[size];
        buffer.get(data);
        var input = CodedInputStream.newInstance(new ByteArrayInputStream(data));
        List<T> deserializedMessages = new ArrayList<>();

        try {
            while (!input.isAtEnd()) {
                int messageSize = input.readFixed32();
                deserializedMessages.add(
                        castToType(messageBuilder
                                .mergeFrom(input.readRawBytes(messageSize))
                                .build()
                        )
                );
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot deserialize object", e);
        }

        return deserializedMessages;
    }
}
