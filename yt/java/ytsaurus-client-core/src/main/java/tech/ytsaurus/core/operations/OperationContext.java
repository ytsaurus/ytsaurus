package tech.ytsaurus.core.operations;

/**
 * This class is used when iterating over table rows in operations.
 */
public class OperationContext {

    private long tableIndex = 0;
    private long rowIndex = 0;

    private long prevTableIndex = 0;
    private long prevRowIndex = 0;

    private boolean setTableIndex;
    private boolean setRowIndex;

    // It's equal to `true` when we read the next row for the future.
    private boolean returnPrevIndexes = false;

    public OperationContext() {
        this.setTableIndex = false;
        this.setRowIndex = false;
    }

    public OperationContext(boolean setTableIndex, boolean setRowIndex) {
        this.setTableIndex = setTableIndex;
        this.setRowIndex = setRowIndex;
    }

    public void setReturnPrevIndexes(boolean returnPrevIndexes) {
        this.returnPrevIndexes = returnPrevIndexes;
    }

    public OperationContext withSettingIndices(boolean setTableIndex, boolean setRowIndex) {
        this.setTableIndex = setTableIndex;
        this.setRowIndex = setRowIndex;
        return this;
    }

    public long getTableIndex() {
        if (returnPrevIndexes) {
            return prevTableIndex;
        }
        return tableIndex;
    }

    public long getRowIndex() {
        if (returnPrevIndexes) {
            return prevRowIndex;
        }
        return rowIndex;
    }

    /**
     * For saving tableIndex of the next row.
     * prevTableIndex saves because in some cases we read the next row for the future.
     */
    public void setTableIndex(long tableIndex) {
        this.prevTableIndex = this.tableIndex;
        this.tableIndex = tableIndex;
    }

    /**
     * For saving rowIndex of the next row.
     * prevRowIndex saves because in some cases we read the next row for the future.
     */
    public void setRowIndex(long rowIndex) {
        this.prevRowIndex = this.rowIndex;
        this.rowIndex = rowIndex;
    }

    /**
     * Whether it is necessary to set the attribute with the table index in the row node.
     */
    public boolean isSettingTableIndex() {
        return setTableIndex;
    }

    /**
     * Whether it is necessary to set the attribute with the row index in the row node.
     */
    public boolean isSettingRowIndex() {
        return setRowIndex;
    }

    @Override
    public String toString() {
        return "{" +
                "tableIndex: " + tableIndex +
                ", rowIndex: " + rowIndex +
                ", setTableIndex: " + setTableIndex +
                ", setRowIndex: " + setRowIndex +
                ", returnPrevIndexes: " + returnPrevIndexes +
                ", prevTableIndex: " + prevTableIndex +
                ", prevRowIndex: " + prevRowIndex +
                "}";
    }
}
