package ru.yandex.yt.rpc.protocol.proto;

import java.util.Arrays;
import java.util.UUID;

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import io.netty.buffer.ByteBufUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.rpc.TResponseHeader;
import ru.yandex.yt.rpc.protocol.bus.BusPackage;
import ru.yandex.yt.rpc.protocol.rpc.RpcMessageType;
import ru.yandex.yt.rpc.utils.Utility;

import static ru.yandex.yt.rpc.utils.Utility.toInt;

/**
 * @author valri
 */
public class ProtobufHelpers {
    private static Logger logger = LoggerFactory.getLogger(ProtobufHelpers.class);

    /**
     * Retrieve request Id from rpc-style message.
     *
     * @param responseHeader    header retrieved from responce
     * @return                  request Id
     */
    public static UUID getUuidFromHeader(TResponseHeader responseHeader) {
        return new UUID(responseHeader.getRequestId().getFirst(), responseHeader.getRequestId().getSecond());
    }

    /**
     * Validate rpc-style header.
     *
     * @param busPackage        incoming bus package
     * @param responseHeader    internal header(which is packed into busPackage)
     * @return                  result of validation
     */
    public static boolean validateHeader(BusPackage busPackage, TResponseHeader responseHeader) {
        final RpcMessageType typeReceived = ProtobufHelpers.getMessageType(busPackage);
        if (typeReceived == null || typeReceived != RpcMessageType.RESPONSE) {
            logger.error("Failed parse response: ({})", busPackage);
            return false;
        }
        if (responseHeader.getError().hasMessage()) {
            logger.error("Failed to execute requestId={}. Error received: `{}`.",
                    getUuidFromHeader(responseHeader), responseHeader.getError().getMessage());
            return false;
        }
        return true;
    }

    /**
     * Retrieves type from rpc-style header. Type is first 4 bytes in part[0](or headerPart).
     *
     * @param busPackage    bus package with rpc-message inside
     * @return              type of the message
     */
    private static RpcMessageType getMessageType(BusPackage busPackage) {
        if (busPackage.getHeaderMessage().size() >= Integer.BYTES) {
            final byte[] type = Utility.byteArrayFromList(busPackage.getHeaderMessage().subList(0, Integer.BYTES));
            final int messageType = ByteBufUtil.swapInt(toInt(type, 0));
            return RpcMessageType.fromType(messageType);
        }
        return null;
    }

    /**
     * Retrieves a message from parts[0](discard first 4 bytes with type).
     *
     * @param busPackage    bus package with rpc-message inside
     * @return              rpc header (TResponseHeader)
     */
    public static TResponseHeader parseResponseHeader(BusPackage busPackage) {
        if (busPackage.getHeaderMessage().size() > Integer.BYTES) {
            try {
                // Skip first byte - TFixedHeader
                // Used for message type which retrieved in getMessageType
                final byte[] header = Utility.byteArrayFromList(busPackage.getHeaderMessage());
                return TResponseHeader.parseFrom(Arrays.copyOfRange(header, Integer.BYTES, header.length));
            } catch (InvalidProtocolBufferException e) {
                logger.error("Failed to parse protobuf header for incoming bus package ({})", busPackage, e);
            }
        }
        return TResponseHeader.getDefaultInstance();
    }

    /**
     * Retrieves protobuf message from bus body(parts[1]).
     *
     * @param busPackage    incoming bus package
     * @param parser        protobuf parser
     * @param clazz         return type
     * @param <T>           type of parser
     * @param <C>           type of return value
     * @return              parsed message in C type
     */
    @SuppressWarnings("unchecked")
    public static <T extends Parser, C extends GeneratedMessageV3> C getProtoFromRsp(BusPackage busPackage, T parser,
                                                                                     Class<C> clazz)
    {
        try {
            if (busPackage.getBodyMessage().size() > 2 * Integer.BYTES) {
                // Skip two first integers - TSerializedMessageFixedHeader
                // Used for coding and encoding in RPC Server
                return (C) parser.parseFrom(Arrays.copyOfRange(
                        Utility.byteArrayFromList(busPackage.getBodyMessage()), 2 * Integer.BYTES,
                        Utility.byteArrayFromList(busPackage.getBodyMessage()).length));
            } else {
                logger.error("Bus body is too small to contain protobuf message.");
            }
        } catch (Exception e) {
            logger.error("Failed to parse body from protobuf for incoming bus package ({})", busPackage, e);
        }
        return null;
    }
}
