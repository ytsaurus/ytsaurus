package ru.yandex.yt.ytclient.proxy.internal;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.bolts.collection.Cf;
import ru.yandex.bolts.collection.Option;
import ru.yandex.misc.ExceptionUtils;
import ru.yandex.yt.rpc.TResponseHeader;
import ru.yandex.yt.rpc.TStreamingFeedbackHeader;
import ru.yandex.yt.rpc.TStreamingPayloadHeader;
import ru.yandex.yt.rpcproxy.TRowsetDescriptor;
import ru.yandex.yt.rpcproxy.TRspReadTable;
import ru.yandex.yt.rpcproxy.TTableReaderPayload;
import ru.yandex.yt.ytclient.object.UnversionedRowsetDeserializer;
import ru.yandex.yt.ytclient.proxy.ApiServiceUtil;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientResponse;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcMessageParser;
import ru.yandex.yt.ytclient.rpc.RpcStreamConsumer;
import ru.yandex.yt.ytclient.rpc.RpcUtil;
import ru.yandex.yt.ytclient.rpc.internal.LazyResponse;
import ru.yandex.yt.ytclient.rpc.internal.RpcServiceMethodDescriptor;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;
import ru.yandex.yt.ytclient.wire.WireProtocolReader;

public class TableReaderImpl implements RpcStreamConsumer {
    private static final Logger logger = LoggerFactory.getLogger(TableReaderImpl.class);

    private final static RpcMessageParser<TRspReadTable> responseParser = RpcServiceMethodDescriptor.makeMessageParser(TRspReadTable.class);
    private final CompletableFuture<RpcClientResponse<TRspReadTable>> result;

    private final RpcClientStreamControl control;
    private long offset = 0;

    private Object metadata = null;

    private TRowsetDescriptor currentRowsetDescriptor = null;
    private TableSchema currentReadSchema = null;
    private UnversionedRowsetDeserializer deserializer = null;

    public TableReaderImpl(RpcClientStreamControl control) {
        this.control = control;
        this.control.subscribe(this);
        this.control.sendEof();
        this.result = new CompletableFuture<>();
    }

    @Override
    public void onFeedback(RpcClient sender, TStreamingFeedbackHeader header, List<byte[]> attachments) {
        // throw new IllegalArgumentException();
        logger.debug("Feedback {} {}", header.getReadPosition(), attachments.size());

        // this.control.sendEof();
    }

    private void parseDescriptorDeltaRefSize(ByteBuffer bb, int size) throws Exception {
        int endPosition = bb.position() + size;
        TRowsetDescriptor rowsetDescriptor = TRowsetDescriptor.parseFrom(bb.slice().limit(size));
        ApiServiceUtil.validateRowsetDescriptor(rowsetDescriptor);

        if (currentReadSchema == null) {
            currentReadSchema = ApiServiceUtil.deserializeRowsetSchema(rowsetDescriptor);
            currentRowsetDescriptor = rowsetDescriptor;
            deserializer = new UnversionedRowsetDeserializer(currentReadSchema);
        } else if (rowsetDescriptor.getColumnsCount() > 0) {
            TRowsetDescriptor.Builder builder = TRowsetDescriptor.newBuilder();
            builder.mergeFrom(currentRowsetDescriptor);
            builder.addAllColumns(rowsetDescriptor.getColumnsList());
            currentRowsetDescriptor = builder.build();
            currentReadSchema = ApiServiceUtil.deserializeRowsetSchema(currentRowsetDescriptor);
            deserializer = new UnversionedRowsetDeserializer(currentReadSchema);
        }

        logger.debug("{}", rowsetDescriptor);
        bb.position(endPosition);
    }

    private void parseMergedRowRefs(ByteBuffer bb, int size) {
        byte[] data = new byte[size];
        bb.get(data);
        UnversionedRowset rowset = new WireProtocolReader(Cf.list(data)).readUnversionedRowset(deserializer).getRowset();
        // logger.debug("{}", rowset);
    }

    private void parseRowData(ByteBuffer bb, int size) throws Exception {
        int endPosition = bb.position() + size;

        int parts = bb.getInt();

        if (parts != 2) {
            throw new IllegalArgumentException();
        }

        int descriptorDeltaRefSize = (int)bb.getLong();
        parseDescriptorDeltaRefSize(bb, descriptorDeltaRefSize);

        int mergedRowRefsSize = (int)bb.getLong();
        parseMergedRowRefs(bb, mergedRowRefsSize);

        if (bb.position() != endPosition) {
            throw new IllegalArgumentException();
        }
    }

    private void parsePayload(ByteBuffer bb, int size) throws Exception {
        int endPosition = bb.position() + size;
        TTableReaderPayload payload = TTableReaderPayload.parseFrom(bb.slice().limit(size));
        logger.debug("{}", payload);
        bb.position(endPosition);
    }

    private void parseRowsWithPayload(byte[] attachment) throws Exception {
        ByteBuffer bb = ByteBuffer.wrap(attachment).order(ByteOrder.LITTLE_ENDIAN);
        int parts = bb.getInt();
        if (parts != 2) {
            throw new IllegalArgumentException();
        }

        int rowDataSize = (int)bb.getLong();

        parseRowData(bb, rowDataSize);

        int payloadSize = (int)bb.getLong();

        parsePayload(bb, payloadSize);

        if (bb.hasRemaining()) {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public void onPayload(RpcClient sender, TStreamingPayloadHeader header, List<byte[]> attachments) {

        logger.debug("payload ");

        if (attachments.isEmpty()) {
            throw new IllegalArgumentException();
        }

        if (metadata == null) {
            metadata = attachments.get(0);
            offset += attachments.get(0).length;

            attachments = attachments.subList(1, attachments.size());
        }

        for (byte[] attachment : attachments) {
            if (attachment != null) {
                offset += attachment.length;

                try {
                    parseRowsWithPayload(attachment);
                } catch (Exception ex) {
                    throw ExceptionUtils.translate(ex);
                }

            } else {

                logger.debug("EOF");

                offset += 1;
            }
        }

        logger.debug("offset {} {} ", offset, RpcUtil.fromProto(header.getRequestId()));

        control.feedback(offset);
    }

    @Override
    public void onResponse(RpcClient sender, TResponseHeader header, List<byte[]> attachments) {
        logger.debug("Response");

        if (!result.isDone()) {
            if (attachments.size() < 1 || attachments.get(0) == null) {
                throw new IllegalStateException("Received response without a body");
            }
            result.complete(new LazyResponse<>(responseParser, attachments.get(0),
                    new ArrayList<>(attachments.subList(1, attachments.size())), sender,
                    Option.of(header)));
        }
    }

    @Override
    public void onError(RpcClient sender, Throwable error) {
        logger.error("Error", error);

        if (!result.isDone()) {
            result.completeExceptionally(error);
        }
    }

    public void cancel() {
        control.cancel();
    }

    public void waitResult() throws Exception {
        result.get();
    }
}
