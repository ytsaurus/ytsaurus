package ru.yandex.yt.rpc.protocol.rpc.lookup;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ru.yandex.yt.rpc.client.schema.TableSchema;
import ru.yandex.yt.rpc.utils.Utility;

/**
 * @author valri
 */
public class RpcRspLookupRows extends RpsLookupRspMessage {
    private static Logger logger = LogManager.getLogger(RpcRspLookupRows.class);

    public RpcRspLookupRows(TableSchema tableSchema) {
        this.schema = tableSchema;
    }

    public List<Map<String, Object>> parseRpcResponse(List<List<Byte>> parts) {
        List<Map<String, Object>> resultList = new ArrayList<>();
        List<Byte> totalParts = parts.stream().flatMap(x -> x.stream()).collect(Collectors.toList());
        resultList.addAll(parseBusPart(totalParts));
        return resultList;
    }

    private List<Map<String, Object>> parseBusPart(List<Byte> part) {
        byte[] message = Utility.byteArrayFromList(part);
        ByteBuffer buffer = ByteBuffer.wrap(message);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        long keys = buffer.getLong();
        List<Map<String, Object>> resultList = new ArrayList<>();
        while (keys-- != 0) {
            if (!enoughCapacity(buffer, Long.BYTES)) {
                throw new RuntimeException("Not enough bytes to read incoming header.");
            }
            final long values = buffer.getLong();
            final Map<String, Object> result = new HashMap<>();
            for (int i = 0; i < values; ++i) {
                Pair<String, Object> pair = parseValue(buffer);
                if (pair != null) {
                    result.put(pair.getLeft(), pair.getRight());
                }
            }
            resultList.add(result);
        }
        return resultList;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}
