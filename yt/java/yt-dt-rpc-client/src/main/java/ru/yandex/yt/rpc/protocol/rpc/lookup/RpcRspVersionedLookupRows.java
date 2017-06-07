package ru.yandex.yt.rpc.protocol.rpc.lookup;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.rpc.client.ValueType;
import ru.yandex.yt.rpc.client.responses.VersionedLookupRow;
import ru.yandex.yt.rpc.client.schema.TableSchema;
import ru.yandex.yt.rpc.utils.Utility;

/**
 * @author valri
 */
public class RpcRspVersionedLookupRows extends RpsLookupRspMessage {
    private static Logger logger = LoggerFactory.getLogger(RpcRspVersionedLookupRows.class);

    private static final int SMALLEST_KEY = 8;
    private static final int HEADER_SIZE = 16;
    private static final int KEYS_FOLD = 64;
    private static final int EMPTY_ROW = -1;

    private protocol.ApiService.TRowsetDescriptor desc;

    public RpcRspVersionedLookupRows(protocol.ApiService.TRowsetDescriptor desc, TableSchema schema) {
        this.desc = desc;
        this.schema = schema;
    }

    public List<VersionedLookupRow> parseRpcResponse(List<List<Byte>> parts) {
        List<VersionedLookupRow> resultList = new ArrayList<>();
        for (List<Byte> sub : parts) {
            resultList.addAll(parseBusPart(sub));
        }
        return resultList;
    }

    private List<VersionedLookupRow> parseBusPart(List<Byte> part) {
        byte[] message = Utility.byteArrayFromList(part);
        ByteBuffer buffer = ByteBuffer.wrap(message);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        long numberOfValues = buffer.getLong();
        List<VersionedLookupRow> resultList = new ArrayList<>((int) numberOfValues);
        while (numberOfValues-- > 0) {
            if (!enoughCapacity(buffer, SMALLEST_KEY)) {
                logger.error("Not enough bytes to read number of values and number of keys, "
                        + "there are only {} bytes", buffer.remaining());
                continue;
            }
            int values = buffer.getInt();
            int keys = buffer.getInt();

            if (values ==  EMPTY_ROW && keys == EMPTY_ROW) {
                logger.error("Empty row received. Continue.");
                continue;
            }

            if (!enoughCapacity(buffer, SMALLEST_KEY)) {
                logger.error("Not enough bytes to read timestamps number, there are only {} bytes left.",
                        buffer.remaining());
                continue;
            }
            int writeTimestampsNum = buffer.getInt();
            int deleteTimestampsNum = buffer.getInt();

            VersionedLookupRow row = new VersionedLookupRow();
            if (writeTimestampsNum > 0) {
                if (!enoughCapacity(buffer, writeTimestampsNum * Long.BYTES)) {
                    getLogger().error("Not enough bytes for writeTimestamps array.");
                    break;
                }
                readTimestamps(buffer, writeTimestampsNum, row.getWriteTimestamps());
            }

            if (deleteTimestampsNum > 0) {
                if (!enoughCapacity(buffer, deleteTimestampsNum * Long.BYTES)) {
                    getLogger().error("Not enough bytes for deleteTimestamps array.");
                    break;
                }
                readTimestamps(buffer, deleteTimestampsNum, row.getDeleteTimestamps());
            }

            if (keys > 0) {
                int keysBytesSizeWithFold = (keys % KEYS_FOLD == 0) ? (keys / KEYS_FOLD) : (keys / KEYS_FOLD + 1);
                if (!enoughCapacity(buffer, (keysBytesSizeWithFold * Long.BYTES) + (keys * SMALLEST_KEY))) {
                    logger.error("Not enough bytes for keys bit mask reading.");
                    break;
                }
                Long[] keysBitMask = new Long[keysBytesSizeWithFold];
                for (int i = 0; i < keysBytesSizeWithFold; ++i) {
                    keysBitMask[i] = buffer.getLong();
                }
                for (Long mask : keysBitMask) {
                    for (int keyId = 0; keyId < keys; ++keyId) {
                        if (keyId >= desc.getColumnsList().size()) {
                            logger.error("KeyId is out of columnDescriptor list");
                            continue;
                        }
                        ValueType type = ValueType.fromType((short) desc.getColumnsList().get(keyId).getType());
                        long length = (type != null  && type == ValueType.STRING) ? buffer.getLong() : 0;
                        row.addValue(desc.getColumnsList().get(keyId).getName(), parseKey(buffer, type, (int) length));
                    }
                }
            }

            while (values-- > 0) {
                Pair<String, Object> pair = parseValue(buffer);
                if (pair != null) {
                    Long timestamp = buffer.getLong();
                    row.addValue(pair.getLeft(), new VersionedLookupRow.VersionedValue(pair.getRight(), timestamp));
                }
            }
            resultList.add(row);
        }
        return resultList;
    }

    private void readTimestamps(ByteBuffer buffer, int numOfTimesamps, List<Long> listOfTimestapms) {
        while (--numOfTimesamps >= 0) {
            listOfTimestapms.add(buffer.getLong());
        }
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}
