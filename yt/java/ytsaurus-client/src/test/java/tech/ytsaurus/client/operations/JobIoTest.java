package tech.ytsaurus.client.operations;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import tech.ytsaurus.core.DataSize;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;


public class JobIoTest {
    @Test
    public void testBasic() {
        JobIo jobIo = JobIo.builder()
                .setEnableTableIndex(false)
                .setEnableRowIndex(true)
                .setTableWriter(TableWriterOptions.builder()
                        .setBlockSize(DataSize.fromMegaBytes(1))
                        .setDesiredChunkSize(DataSize.fromMegaBytes(500))
                        .setMaxRowWeight(DataSize.fromMegaBytes(10))
                        .build())
                .build();

        YTreeMapNode prepared = jobIo.prepare();

        Assert.assertTrue(prepared.get("control_attributes").isPresent());
        Map<String, YTreeNode> controlAttributes = prepared.get("control_attributes").get().asMap();
        Assert.assertFalse(controlAttributes.get("enable_table_index").boolValue());
        Assert.assertTrue(controlAttributes.get("enable_row_index").boolValue());

        Assert.assertTrue(prepared.get("table_writer").isPresent());
        Map<String, YTreeNode> tableWriter = prepared.get("table_writer").get().asMap();
        Assert.assertEquals(1024 * 1024, tableWriter.get("block_size").intValue());
        Assert.assertEquals(500 * 1024 * 1024, tableWriter.get("desired_chunk_size").intValue());
        Assert.assertEquals(10 * 1024 * 1024, tableWriter.get("max_row_weight").intValue());
    }
}
