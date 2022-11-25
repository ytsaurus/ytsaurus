package tech.ytsaurus.client;

import javax.annotation.Nonnull;

import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.core.tables.TableSchema;

/**
 * Row modification request that uses {@link UnversionedRow} as table row representation
 *
 * @see UnversionedRow
 */
public class ModifyRowsRequest extends tech.ytsaurus.client.request.ModifyRowsRequest.BuilderBase<ModifyRowsRequest> {
    public ModifyRowsRequest(String path, TableSchema schema) {
        setPath(path).setSchema(schema);
    }

    @Nonnull
    @Override
    protected ModifyRowsRequest self() {
        return this;
    }

    @Override
    public tech.ytsaurus.client.request.ModifyRowsRequest build() {
        return new tech.ytsaurus.client.request.ModifyRowsRequest(this);
    }
}
