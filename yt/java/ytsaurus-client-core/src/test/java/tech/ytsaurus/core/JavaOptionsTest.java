package tech.ytsaurus.core;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JavaOptionsTest {
    @Test
    public void test() {
        JavaOptions options = JavaOptions.empty();
        assertTrue(options.getOptions().isEmpty());

        options = options.withOption("-XX:-AllowUserSignalHandlers");
        assertEquals(List.of("-XX:-AllowUserSignalHandlers"), options.getOptions());

        options = options.withOption("-XX:AltStackSize=16384");
        assertEquals(List.of("-XX:-AllowUserSignalHandlers", "-XX:AltStackSize=16384"), options.getOptions());

        options = options.withoutOption("-XX:-AllowUserSignalHandlers");
        assertEquals(List.of("-XX:AltStackSize=16384"), options.getOptions());

        options = options.withOption("-XX:AltStackSize=1");
        assertEquals(List.of("-XX:AltStackSize=1"), options.getOptions());

        options = options.withoutOption("-XX:AltStackSize=16384");
        assertTrue(options.getOptions().isEmpty());

        options = options.withoutOption("-XX:AltStackSize=16384").withoutOptionStartsWith("-XX:AltStackSize");
        assertTrue(options.getOptions().isEmpty());

        options = options.withMemory(DataSize.fromMegaBytes(1));
        assertEquals(List.of("-Xms1m", "-Xmx1m"), options.getOptions());

        options = options.withoutOption("-XXX");
        assertEquals(List.of("-Xms1m", "-Xmx1m"), options.getOptions());

        options = options.withoutOptionStartsWith("-Xms");
        assertEquals(List.of("-Xmx1m"), options.getOptions());

        options = options.withMemory(DataSize.fromMegaBytes(1), DataSize.fromMegaBytes(2));
        assertEquals(List.of("-Xms1m", "-Xmx2m"), options.getOptions());
    }
}
