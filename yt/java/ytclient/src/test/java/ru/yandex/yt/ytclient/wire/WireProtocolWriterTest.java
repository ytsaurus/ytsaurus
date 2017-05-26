package ru.yandex.yt.ytclient.wire;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import ru.yandex.yt.ytclient.tables.ColumnValueType;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class WireProtocolWriterTest extends WireProtocolTest {
    @Test
    public void writeUnversionedRow() {
        WireProtocolWriter writer = new WireProtocolWriter();
        writer.writeUnversionedRow(makeUnversionedRowSample());
        byte[] data = mergeChunks(writer.finish());
        assertThat(data, dataEquals(makeUnversionedRowCanonicalBlob()));
    }

    @Test
    public void writeSchemafulRow() {
        WireProtocolWriter writer = new WireProtocolWriter();
        writer.writeSchemafulRow(makeSchemafulRowSample());
        byte[] data = mergeChunks(writer.finish());
        assertThat(data, dataEquals(makeSchemafulRowCanonicalBlob()));
    }

    @Test
    public void writeVeryBigRow() {
        // Тест на регрессию баги с перепутанными Math.min и Math.max
        byte[] data = new byte[32768]; // строчка нулей в 32КБ
        WireProtocolWriter writer = new WireProtocolWriter();
        writer.writeUnversionedRow(new UnversionedRow(Collections.singletonList(
                new UnversionedValue(0, ColumnValueType.STRING, false, data)
        )));
        List<byte[]> chunks = writer.finish();
        // Проверяем, что на выходе получился 1 чанк
        assertThat(chunks.size(), is(1));
        // Проверяем, что на выходе 16 (кол-во колонок и заголовок колонки) + 32768 байт
        assertThat(chunks.get(0).length, is(16 + 32768));
    }

    @Test
    public void writeManySmallRows() {
        // Тест на регрессию баги с неправильным использованием ByteBuffer.wrap
        byte[] data = new byte[1024]; // строчка в 1KB
        WireProtocolWriter writer = new WireProtocolWriter();
        for (int i = 0; i < 32; ++i) {
            writer.writeUnversionedRow(new UnversionedRow(Collections.singletonList(
                    new UnversionedValue(0, ColumnValueType.STRING, false, data)
            )));
        }
        List<byte[]> chunks = writer.finish();
        // Проверяем, что на выходе получился 1 чанк
        assertThat(chunks.size(), is(1));
        // Проверяем, что на выходе каждой строчки 16 (кол-во колонок и заголовок колонки) + 1024 байта
        assertThat(chunks.get(0).length, is(32 * (16 + 1024)));
    }
}
