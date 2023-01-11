package tech.ytsaurus.core;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class JavaOptionsTest {
    @Test
    public void test() {
        JavaOptions options = JavaOptions.empty();
        Assert.assertTrue(options.getOptions().isEmpty());

        options = options.withOption("-XX:-AllowUserSignalHandlers");
        Assert.assertEquals(List.of("-XX:-AllowUserSignalHandlers"), options.getOptions());

        options = options.withOption("-XX:AltStackSize=16384");
        Assert.assertEquals(List.of("-XX:-AllowUserSignalHandlers", "-XX:AltStackSize=16384"), options.getOptions());

        options = options.withoutOption("-XX:-AllowUserSignalHandlers");
        Assert.assertEquals(List.of("-XX:AltStackSize=16384"), options.getOptions());

        options = options.withOption("-XX:AltStackSize=1");
        Assert.assertEquals(List.of("-XX:AltStackSize=1"), options.getOptions());

        options = options.withoutOption("-XX:AltStackSize=16384");
        Assert.assertTrue(options.getOptions().isEmpty());

        options = options.withoutOption("-XX:AltStackSize=16384").withoutOptionStartsWith("-XX:AltStackSize");
        Assert.assertTrue(options.getOptions().isEmpty());

        options = options.withMemory(DataSize.fromMegaBytes(1));
        Assert.assertEquals(List.of("-Xms1m", "-Xmx1m"), options.getOptions());

        options = options.withoutOption("-XXX");
        Assert.assertEquals(List.of("-Xms1m", "-Xmx1m"), options.getOptions());

        options = options.withoutOptionStartsWith("-Xms");
        Assert.assertEquals(List.of("-Xmx1m"), options.getOptions());

        options = options.withMemory(DataSize.fromMegaBytes(1), DataSize.fromMegaBytes(2));
        Assert.assertEquals(List.of("-Xms1m", "-Xmx2m"), options.getOptions());
    }
}
