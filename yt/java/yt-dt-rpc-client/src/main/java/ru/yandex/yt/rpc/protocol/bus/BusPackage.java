package ru.yandex.yt.rpc.protocol.bus;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;

import static ru.yandex.yt.rpc.utils.Utility.byteArrayFromList;


/**
 * @author valri
 */
public class BusPackage {
    public static final int MAX_PART_COUNT = 1 << 28;
    public static final int MAX_PART_SIZE = 1024 * 1024;
    public static final int SMALLEST_BUS_PACKAGE_BYTES_LEN = 36;
    public static final int SMALLEST_BUS_PACKAGE_BYTES_LEN_NO_SUM = 36 - Long.BYTES;
    public static final int DEFAULT_SIGNATURE = 0x78616d4f;
    public static final long NULL_CHECKSUM = 0;

    private int signature = DEFAULT_SIGNATURE;
    private short type;
    private short flags;
    private long checkSum = NULL_CHECKSUM;
    private UUID uuid = UUID.randomUUID();
    private List<List<Byte>> message = new ArrayList<>();

    public BusPackage(final PacketType packetType, final PacketFlags flags, final UUID uuid,
                      final boolean enableChecksum, final List<List<Byte>> message)
    {
        this.type = packetType.getValue();
        this.flags = flags.getValue();
        this.uuid = uuid;
        this.message.addAll(message.stream().map(ArrayList::new).collect(Collectors.toList()));
        if (enableChecksum) {
            //make checksum
        }
    }

    public BusPackage(final int signature, final short packetType, final short flags, final UUID uid,
                      final long checkSum, final List<List<Byte>> message)
    {
        this.signature = signature;
        this.type = packetType;
        this.flags = flags;
        this.uuid = uid;
        this.checkSum = checkSum;
        this.message.addAll(message.stream().map(ArrayList::new).collect(Collectors.toList()));
    }

    public BusPackage(final BusPackage bus) {
        this.signature = bus.signature;
        this.type = bus.type;
        this.flags = bus.flags;
        this.uuid = bus.uuid;
        this.checkSum = bus.checkSum;
        message.addAll(bus.message.stream().map(ArrayList::new).collect(Collectors.toList()));
    }

    public BusPackage makeACK() {
        this.type = PacketType.ACK.getValue();
        message.clear();
        this.checkSum = NULL_CHECKSUM;
        return this;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "signature: {0}, type: {1}, flags: {2}, uuid: {3}, parts: {4}",
                this.signature, this.type, this.flags, this.uuid, this.message.size());
    }

    private int getPackageSizeInBytes() {
        int size = SMALLEST_BUS_PACKAGE_BYTES_LEN;
        if (!this.message.isEmpty()) {
            size += this.message.size() * (Integer.BYTES + Long.BYTES);
            size += Long.BYTES;
            for (List<Byte> l : this.message) {
                size += l.size();
            }
        }
        return size;
    }

    public byte[] getBytes() {
        final ByteBuffer out = ByteBuffer.allocate(this.getPackageSizeInBytes());
        out.order(ByteOrder.LITTLE_ENDIAN);
        out.putInt(signature);
        out.putShort(this.getType().getValue());
        out.putShort(this.getFlags().getValue());
        out.putLong(uuid.getMostSignificantBits());
        out.putLong(uuid.getLeastSignificantBits());
        out.putInt(message.size());
        out.putLong(checkSum);
        if (!message.isEmpty() && message.size() < BusPackage.MAX_PART_COUNT) {
            final List<Byte> blob = new ArrayList<>();
            message.stream().filter(l -> l.size() < BusPackage.MAX_PART_SIZE).forEach(blob::addAll);
            for (List<Byte> l : message) {
                out.putInt(l.size());
            }
            for (List<Byte> l : message) {
                out.putLong(BusPackage.NULL_CHECKSUM);
            }
            out.putLong(BusPackage.NULL_CHECKSUM);
            out.put(byteArrayFromList(blob));
        }
        return out.array();
    }

    public int getNumberOfParts() {
        return message.size();
    }

    public int getSignature() {
        return signature;
    }

    public PacketType getType() {
        return PacketType.fromType(type);
    }

    public void setType(PacketType type) {
        this.type = type.getValue();
    }

    public PacketFlags getFlags() {
        return PacketFlags.fromFlag(flags);
    }

    public long getCheckSum() {
        return checkSum;
    }

    public UUID getUuid() {
        return uuid;
    }

    public List<Byte> getHeaderMessage() {
        return  message.size() > 0 ? message.get(0) : Collections.emptyList();
    }

    public List<Byte> getBodyMessage() {
        return message.size() > 1 ? message.get(1) : Collections.emptyList();
    }

    public List<List<Byte>> getBlobPart() {
        return message.size() > 2 ? message.subList(2, message.size()) : Collections.emptyList();
    }

    public boolean hasBlobPart() {
        return message.size() > 2;
    }

    public enum PacketType {
        MESSAGE((short) 0), ACK((short) 1);
        private final short value;

        private static final Map<Short, PacketType> LOOKUP = Maps.uniqueIndex(
                Arrays.asList(PacketType.values()),
                PacketType::getValue
        );

        PacketType(short value) {
            this.value = value;
        }

        public short getValue() {
            return value;
        }

        public static PacketType fromType(short status) {
            return LOOKUP.get(status);
        }
    }

    public enum PacketFlags {
        NONE((short) 0), REQUEST_ACK((short) 1);
        private final short value;
        private static final Map<Short, PacketFlags> LOOKUP = Maps.uniqueIndex(
                Arrays.asList(PacketFlags.values()),
                PacketFlags::getValue
        );

        PacketFlags(short value) {
            this.value = value;
        }

        public short getValue() {
            return value;
        }

        public static PacketFlags fromFlag(short flag) {
            return LOOKUP.get(flag);
        }
    }
}
