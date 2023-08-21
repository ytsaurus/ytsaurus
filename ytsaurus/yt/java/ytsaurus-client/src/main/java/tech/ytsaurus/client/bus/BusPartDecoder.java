package tech.ytsaurus.client.bus;

import io.netty.buffer.ByteBuf;

/**
 * Вспомогательный класс для вычитывания частей сообщения
 */
class BusPartDecoder {
    private byte[] data;
    private int size;

    BusPartDecoder() {
        // nothing
    }

    /**
     * Подготавливается к чтению части размером size байт
     */
    public void start(int size) {
        if (size < 1) {
            throw new IllegalArgumentException("size must be positive");
        }
        this.data = new byte[size];
        this.size = 0;
    }

    /**
     * Читает необходимое кол-во байт из in
     * <p>
     * Возвращает массив байт, если все необходимые данные прочитаны
     * Возвращает null, если данных не хватает (из in при этом прочитаны все доступные данные)
     */
    public byte[] read(ByteBuf in) {
        int available = in.readableBytes();
        if (available >= data.length - size) {
            in.readBytes(data, size, data.length - size);
            byte[] part = data;
            data = null;
            return part;
        }
        in.readBytes(data, size, available);
        size += available;
        return null;
    }
}
