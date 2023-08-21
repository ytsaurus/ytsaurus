package tech.ytsaurus.client.operations;

import java.io.InputStream;
import java.io.OutputStream;

import tech.ytsaurus.core.YtFormat;
import tech.ytsaurus.core.operations.CloseableIterator;
import tech.ytsaurus.core.operations.OperationContext;
import tech.ytsaurus.core.operations.YTreeBinarySerializer;
import tech.ytsaurus.core.operations.Yield;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeStringNode;


/**
 * For reading/writing of YTreeMapNode using binary yson format.
 *
 * @see YTableEntryType
 */
@NonNullApi
@NonNullFields
public class YsonTableEntryType implements YTableEntryType<YTreeMapNode> {

    private final boolean setTableIndex;
    private final boolean setRowIndex;

    public YsonTableEntryType(boolean setTableIndex, boolean setRowIndex) {
        this.setTableIndex = setTableIndex;
        this.setRowIndex = setRowIndex;
    }

    @Override
    public YTreeStringNode format(FormatContext context) {
        return YtFormat.YSON_BINARY;
    }

    @Override
    public CloseableIterator<YTreeMapNode> iterator(InputStream input, OperationContext context) {
        context.withSettingIndices(setTableIndex, setRowIndex);
        return YTreeBinarySerializer.iterator(input, context).map(YTreeNode::mapNode);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Yield<YTreeMapNode> yield(OutputStream[] output) {
        return (Yield) YTreeBinarySerializer.yield(output);
    }

}
