package tech.ytsaurus.flow.stream;

import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.codec.ByteStringCodec;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeConvertible;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Represents a stream in the Flow framework: a uniquely identified channel of messages of
 * type {@code T} described by a {@link TableSchema}, with a (de)serialization {@link ByteStringCodec<T>}.
 *
 * @param <T> the type of messages in this stream
 */
public interface FlowStream<T> extends YTreeConvertible {

    /**
     * Returns the unique identifier for this stream. Each stream ID must have a corresponding
     * definition in the pipeline spec.
     *
     * @return the stream identifier
     */
    String getStreamId();

    /**
     * Returns the YTsaurus table schema describing messages of this stream.
     *
     * @return the table schema for this stream
     */
    TableSchema getSchema();

    /**
     * Returns the Java class of messages of this stream.
     *
     * @return the message class
     */
    Class<T> getMessageClass();

    /**
     * Returns the codec used to serialize/deserialize messages of this stream.
     *
     * @return the codec
     */
    ByteStringCodec<T> getCodec();

    @Override
    default YTreeNode toYTree() {
        return YTree.builder()
                .beginMap()
                .key("stream_id").value(getStreamId())
                .key("schema").value(getSchema().toYTree())
                .endMap()
                .build();
    }
}
