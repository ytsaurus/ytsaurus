package tech.ytsaurus.client.request;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

import com.google.protobuf.ByteString;
import org.junit.Test;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.ERowsetFormat;
import tech.ytsaurus.rpcproxy.TReqReadTable;
import tech.ytsaurus.rpcproxy.TReqReadTablePartition;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;

import static org.junit.Assert.assertEquals;

public class ArrowReadRequestTest {
    @Test
    public void readTableUsesGenericArrowFormat() {
        TReqReadTable request = new ReadTable<>(
                YPath.simple("//tmp/table"),
                ReadSerializationContext.binaryArrow()
        ).writeTo(TReqReadTable.newBuilder()).build();

        assertEquals(ERowsetFormat.RF_FORMAT, request.getDesiredRowsetFormat());
        assertArrowFormat(request.getFormat());
    }

    @Test
    public void readTablePartitionUsesGenericArrowFormat() {
        CreateTablePartitionReader<ByteBuffer> readRequest = CreateTablePartitionReader
                .binaryArrowBuilder()
                .setCookie(new TablePartitionCookie(ByteString.EMPTY))
                .build();

        TReqReadTablePartition.Builder builder = TReqReadTablePartition.newBuilder();
        readRequest.writeTo(builder);
        TReqReadTablePartition request = builder.build();

        assertEquals(ERowsetFormat.RF_FORMAT, request.getDesiredRowsetFormat());
        assertArrowFormat(request.getFormat());
    }

    @Test
    public void partitionCookiePreservesPayloadDuringSerialization() throws Exception {
        var cookie = new TablePartitionCookie(ByteString.copyFromUtf8("cookie"));
        var output = new ByteArrayOutputStream();
        try (var stream = new ObjectOutputStream(output)) {
            stream.writeObject(cookie);
        }

        TablePartitionCookie restored;
        try (var stream = new ObjectInputStream(new ByteArrayInputStream(output.toByteArray()))) {
            restored = (TablePartitionCookie) stream.readObject();
        }

        assertEquals(cookie.getPayload(), restored.getPayload());
    }

    private static void assertArrowFormat(ByteString serializedFormat) {
        assertEquals(
                "arrow",
                YTreeBinarySerializer.deserialize(serializedFormat.newInput()).stringValue()
        );
    }
}
