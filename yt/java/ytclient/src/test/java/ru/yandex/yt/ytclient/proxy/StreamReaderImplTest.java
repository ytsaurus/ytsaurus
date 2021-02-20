package ru.yandex.yt.ytclient.proxy;

import java.util.Collections;

import com.google.protobuf.Parser;
import org.junit.Test;

import ru.yandex.yt.TGuid;
import ru.yandex.yt.rpc.TStreamingPayloadHeader;
import ru.yandex.yt.rpcproxy.TRspReadTable;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class StreamReaderImplTest {
    private final byte[] bytes0 = {1, 2, 3, 4};
    private final byte[] bytes1 = {5, 6, 7, 8};
    private final byte[] bytes2 = {9, 10, 11, 12};

    @Test
    public void testReadInStraightOrder() throws Exception {
        RpcClient client = mock(RpcClient.class);
        StreamReaderImpl<TRspReadTable> reader = createReader();

        payload(bytes0, 0, reader, client);
        readAndCheck(bytes0, reader);
        checkNotReady(reader);
        payload(bytes1, 1, reader, client);
        readAndCheck(bytes1, reader);
        checkNotReady(reader);
        payload(bytes2, 2, reader, client);
        readAndCheck(bytes2, reader);
        checkNotReady(reader);
        payload(null, 3, reader, client);
        readAndCheck(null, reader);
        assertThat(reader.doCanRead(), is(false));
    }

    @Test
    public void testReadInRandomOrder() throws Exception {
        RpcClient client = mock(RpcClient.class);
        StreamReaderImpl<TRspReadTable> reader = createReader();

        payload(bytes2, 2, reader, client);
        checkNotReady(reader);
        payload(null, 3, reader, client);
        checkNotReady(reader);
        payload(bytes0, 0, reader, client);
        readAndCheck(bytes0, reader);
        checkNotReady(reader);
        payload(bytes1, 1, reader, client);
        readAndCheck(bytes1, reader);
        readAndCheck(bytes2, reader);
        readAndCheck(null, reader);
        assertThat(reader.doCanRead(), is(false));
    }

    private StreamReaderImpl<TRspReadTable> createReader() {
        RpcClientStreamControl control = mock(RpcClientStreamControl.class);
        StreamReaderImpl<TRspReadTable> result = new StreamReaderImpl<>() {
            @Override
            protected Parser<TRspReadTable> responseParser() {
                return null;
            }
        };

        result.onStartStream(control);
        return result;
    }

    private void payload(byte[] bytes,
                         int sequenceNumber,
                         StreamReaderImpl<TRspReadTable> reader,
                         RpcClient client) {
        reader.onPayload(client, createHeader(sequenceNumber), Collections.singletonList(bytes));
    }

    private void readAndCheck(byte[] bytes,
                              StreamReaderImpl<TRspReadTable> reader) throws Exception {
        assertThat(reader.doCanRead(), is(true));
        assertThat(reader.getReadyEvent().isDone(), is(true));
        assertThat(reader.doRead(), is(bytes));
    }

    private void checkNotReady(StreamReaderImpl<TRspReadTable> reader) {
        assertThat(reader.doCanRead(), is(true));
        assertThat(reader.getReadyEvent().isDone(), is(false));
    }

    private TStreamingPayloadHeader createHeader(int sequenceNumber) {
        return TStreamingPayloadHeader
                .newBuilder()
                .setSequenceNumber(sequenceNumber)
                .setCodec(0)
                .setRequestId(TGuid.newBuilder().setFirst(1).setSecond(1).build())
                .setService("test")
                .setMethod("test")
                .build();
    }

}
