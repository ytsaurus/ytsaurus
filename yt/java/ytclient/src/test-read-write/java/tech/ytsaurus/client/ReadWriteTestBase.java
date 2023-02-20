package tech.ytsaurus.client;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.persistence.Entity;

import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.runners.Parameterized;
import tech.ytsaurus.client.bus.BusConnector;
import tech.ytsaurus.client.bus.DefaultBusConnector;
import tech.ytsaurus.client.rpc.Compression;
import tech.ytsaurus.client.rpc.RpcCompression;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;

import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;

public class ReadWriteTestBase {
    YtClient yt;

    public ReadWriteTestBase(YtClient yt) {
        this.yt = yt;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() throws IOException {
        BusConnector connector = new DefaultBusConnector(new NioEventLoopGroup(0), true);
        String localProxy = System.getenv("YT_PROXY");

        YtClient yt = new YtClient(
                connector,
                List.of(new YtCluster(localProxy)),
                "default",
                null,
                YTsaurusClientAuth.builder()
                        .setUser("root")
                        .setToken("")
                        .build(),
                new RpcCompression().setRequestCodecId(Compression.None),
                new RpcOptions());

        YtClient ytWithCompression = new YtClient(
                connector,
                List.of(new YtCluster(localProxy)),
                "default",
                null,
                YTsaurusClientAuth.builder()
                        .setUser("root")
                        .setToken("")
                        .build(),
                new RpcCompression().setRequestCodecId(Compression.Zlib_6),
                new RpcOptions());

        yt.waitProxies().join();

        ytWithCompression.waitProxies().join();

        return List.of(
                new Object[]{yt},
                new Object[]{ytWithCompression});
    }

    String getDigestString(MessageDigest md) {
        byte[] digest = md.digest();
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) {
            sb.append(Integer.toHexString((b & 0xFF) | 0x100), 1, 3);
        }

        return sb.toString();
    }

    @Entity
    @YTreeObject
    static class Row {
        final String key;
        final String value;
        final long intValue;

        Row(String key, String value, long intValue) {
            this.key = key;
            this.value = value;
            this.intValue = intValue;
        }

        Row() {
            this.key = null;
            this.value = null;
            this.intValue = 0;
        }
    }

    static class ObjectsRowsGenerator {
        private long currentRowNumber = 0;

        List<Row> nextRows() {
            if (currentRowNumber >= 10000) {
                return null;
            }

            List<Row> rows = new ArrayList<>();

            for (int i = 0; i < 10; ++i) {
                String key = "key-" + currentRowNumber;
                String value = "value-" + currentRowNumber;

                rows.add(new Row(key, value, currentRowNumber));

                currentRowNumber += 1;
            }

            return rows;
        }

        long rowsCount() {
            return currentRowNumber;
        }
    }
}
