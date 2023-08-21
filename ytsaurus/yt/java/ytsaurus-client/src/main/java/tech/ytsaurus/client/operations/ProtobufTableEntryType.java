package tech.ytsaurus.client.operations;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import tech.ytsaurus.core.common.YTsaurusProtobufFormat;
import tech.ytsaurus.core.operations.CloseableIterator;
import tech.ytsaurus.core.operations.OperationContext;
import tech.ytsaurus.core.operations.Yield;
import tech.ytsaurus.ysontree.YTreeStringNode;

import static tech.ytsaurus.core.utils.ClassUtils.castToType;

public class ProtobufTableEntryType<T extends Message> implements YTableEntryType<T> {
    private static final int CONTROL_ATTR_TABLE_INDEX = -1;
    private static final int CONTROL_ATTR_KEY_SWITCH = -2;
    private static final int CONTROL_ATTR_RANGE_INDEX = -3;
    private static final int CONTROL_ATTR_ROW_INDEX = -4;
    private final Message.Builder builder;
    private final boolean trackIndices;
    private final boolean isInputType;

    public ProtobufTableEntryType(
            Message.Builder messageBuilder,
            boolean trackIndices,
            boolean isInputType
    ) {
        this.builder = messageBuilder;
        this.trackIndices = trackIndices;
        this.isInputType = isInputType;
    }

    @Override
    public YTreeStringNode format(FormatContext context) {
        ArrayList<Message.Builder> messageBuilders = new ArrayList<>();
        int tablesCount = isInputType ?
                context.getInputTableCount().orElseThrow(IllegalArgumentException::new) :
                context.getOutputTableCount().orElseThrow(IllegalArgumentException::new);
        for (int i = 0; i < tablesCount; ++i) {
            messageBuilders.add(builder);
        }
        return new YTsaurusProtobufFormat(messageBuilders).spec();
    }

    @Override
    public CloseableIterator<T> iterator(InputStream input, OperationContext context) {
        return new CloseableIterator<>() {
            final CodedInputStream in = CodedInputStream.newInstance(input);
            boolean hasNextChecked = false;
            @Nullable
            T next;
            int size = 0;
            long rowIndex = 0;
            int tableIndex = 0;

            @Override
            public boolean hasNext() {
                hasNextChecked = true;
                if (next != null) {
                    return true;
                }

                try {
                    if (in.isAtEnd()) {
                        return false;
                    }

                    builder.clear();
                    in.resetSizeCounter();

                    size = in.readFixed32();
                    if (trackIndices) {
                        rowIndex++;
                    }

                    while (size < 0) {
                        switch (size) {
                            case CONTROL_ATTR_KEY_SWITCH:
                                size = in.readFixed32();
                                break;
                            case CONTROL_ATTR_TABLE_INDEX:
                                tableIndex = in.readFixed32();
                                size = in.readFixed32();
                                break;
                            case CONTROL_ATTR_RANGE_INDEX:
                                in.readFixed32();
                                size = in.readFixed32();
                                break;
                            case CONTROL_ATTR_ROW_INDEX:
                                rowIndex = in.readFixed64();
                                size = in.readFixed32();
                                break;
                            default:
                                throw new RuntimeException("broken stream");
                        }
                    }

                    if (trackIndices) {
                        context.setRowIndex(rowIndex);
                        context.setTableIndex(tableIndex);
                    }

                    next = castToType(builder
                            .mergeFrom(in.readRawBytes(size))
                            .build()
                    );
                    return true;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public T next() {
                if (!hasNextChecked && !hasNext()) {
                    throw new IllegalStateException();
                }
                if (next == null) {
                    throw new NoSuchElementException();
                }
                T ret = next;
                next = null;
                hasNextChecked = false;
                return ret;
            }

            @Override
            public void close() throws Exception {
                input.close();
            }
        };
    }

    @Override
    public Yield<T> yield(OutputStream[] output) {
        CodedOutputStream[] writers = new CodedOutputStream[output.length];
        for (int i = 0; i < output.length; ++i) {
            writers[i] = CodedOutputStream.newInstance(output[i]);
        }

        return new Yield<>() {
            @Override
            public void yield(int index, Message value) {
                try {
                    byte[] messageBytes = value.toByteArray();
                    writers[index].writeFixed32NoTag(messageBytes.length);
                    writers[index].writeRawBytes(messageBytes);
                    writers[index].flush();
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }

            @Override
            public void close() throws IOException {
                for (OutputStream stream : output) {
                    stream.close();
                }
            }
        };
    }
}
