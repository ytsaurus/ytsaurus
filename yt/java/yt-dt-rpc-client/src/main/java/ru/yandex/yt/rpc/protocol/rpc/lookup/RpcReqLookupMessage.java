package ru.yandex.yt.rpc.protocol.rpc.lookup;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import com.google.common.primitives.Bytes;
import org.slf4j.Logger;

import ru.yandex.yt.rpc.client.ValueType;
import ru.yandex.yt.rpc.client.requests.LookupReqInfo;
import ru.yandex.yt.rpc.protocol.RpcRequestMessage;

/**
 * @author valri
 */
public abstract class RpcReqLookupMessage extends RpcRequestMessage {
    private static final int STRING_FOLD = 8;

    protected LookupReqInfo info;
    int wholePackSize;

    @Override
    public List<List<Byte>> getBusEnvelope() {
        final List<List<Byte>> fullRequest = super.getBusEnvelope();
        fullRequest.add(createAttachment());
        return fullRequest;
    }

    protected int getPackageSize() {
        int fullSize = Long.BYTES;
        for (Map<String, Object> subFilter : info.filters) {
            fullSize += Long.BYTES;
            for (Map.Entry<String, Object> entry : subFilter.entrySet()) {
                fullSize += 2 * Short.BYTES + Integer.BYTES;
                if (entry.getValue() instanceof String) {
                    int lengthWith = ((String) entry.getValue()).getBytes(Charset.forName("UTF-8")).length;
                    if (lengthWith % STRING_FOLD != 0) lengthWith += STRING_FOLD - (lengthWith % STRING_FOLD);
                    fullSize += lengthWith;
                } else {
                    fullSize += Long.BYTES;
                }
            }
        }
        return fullSize;
    }

    protected List<Byte> createAttachment() {
        final ByteBuffer wholePack = ByteBuffer.allocate(this.wholePackSize);
        wholePack.order(ByteOrder.LITTLE_ENDIAN);
        wholePack.putLong(info.filters.size());
        for (Map<String, Object> subFilter: info.filters) {
            wholePack.putLong(subFilter.size());
            for (Map.Entry<String, Object> entry : subFilter.entrySet()) {
                final Short columnid = info.tableSchema.initialSequence.get(entry.getKey());
                final Object currentObject = entry.getValue();
                if (columnid == null || currentObject == null) {
                    getLogger().error("Null object received as filter");
                    continue;
                }
                wholePack.putShort(columnid);
                final ValueType type = info.tableSchema.nameToType.get(entry.getKey());
                if (type == null) {
                    getLogger().error("Column type for column `{}` not found in table schema", entry.getKey());
                    continue;
                }
                wholePack.putShort(type.getValue());
                switch (type) {
                    case ANY: {
                        /*
                         * Custom type resolver
                         * NULL is not supported
                         */
                        if (currentObject instanceof String) {
                            wholePack.putInt((((String) currentObject).getBytes().length));
                            wholePack.put(((String) currentObject).getBytes());
                            final int strLength = ((String) currentObject).getBytes().length;
                            if (strLength % STRING_FOLD != 0) {
                                wholePack.position(wholePack.position() + (STRING_FOLD - (strLength % STRING_FOLD)));
                            }
                        } else {
                            wholePack.putInt(0);
                            if (entry.getValue() instanceof Integer) {
                                wholePack.putLong((Integer) entry.getValue());
                            } else if (entry.getValue() instanceof Long) {
                                wholePack.putLong((Long) entry.getValue());
                            } else if (entry.getValue() instanceof Double) {
                                wholePack.putLong(((Double) entry.getValue()).longValue());
                            } else if (entry.getValue() instanceof Boolean) {
                                wholePack.putLong(((Boolean) entry.getValue()) ? 1L : 0L);
                            } else if (entry.getValue() instanceof String) {
                                wholePack.put(((String) entry.getValue()).getBytes());
                                final int strLength = ((String) entry.getValue()).getBytes().length;
                                if (strLength % STRING_FOLD != 0) {
                                    wholePack.position(wholePack.position()
                                            + (STRING_FOLD - (strLength % STRING_FOLD)));
                                }
                            }
                        }
                        break;
                    }

                    case STRING: {
                        wholePack.putInt((((String) currentObject).getBytes().length));
                        wholePack.put(((String) currentObject).getBytes());
                        final int strLength = ((String) currentObject).getBytes().length;
                        if (strLength % STRING_FOLD != 0) {
                            wholePack.position(wholePack.position() + (STRING_FOLD - (strLength % STRING_FOLD)));
                        }
                        break;
                    }

                    case INT_64:
                    case UINT_64: {
                        wholePack.putInt(0);
                        if (currentObject instanceof Integer) {
                            wholePack.putLong((Integer) currentObject);
                        } else if (currentObject instanceof Long) {
                            wholePack.putLong((Long) currentObject);
                        }
                        break;
                    }

                    case BOOLEAN: {
                        wholePack.putInt(0);
                        wholePack.putLong(((Boolean) currentObject) ? 1L : 0L);
                        break;
                    }

                    case DOUBLE: {
                        wholePack.putInt(0);
                        wholePack.putLong(((Double) currentObject).longValue());
                        break;
                    }

                    case NULL: {
                        wholePack.putInt(0);
                        break;
                    }

                    case MIN:
                    case THE_BOTTOM:
                    case MAX:
                    default: {
                        getLogger().error("Passed object of wrong type.");
                    }
                }
            }
        }
        return Bytes.asList(wholePack.array());
    }

    public abstract byte[] getRequestBytes();

    protected abstract Logger getLogger();
}
