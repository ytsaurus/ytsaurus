#include "table_upload_options.h"
#include "helpers.h"

#include <yt/client/ypath/rich.h>

#include <yt/core/ytree/helpers.h>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NCompression;
using namespace NCypressClient;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TTableSchema TTableUploadOptions::GetUploadSchema() const
{
    switch (SchemaModification) {
        case ETableSchemaModification::None:
            return TableSchema;

        case ETableSchemaModification::UnversionedUpdate:
            return TableSchema.ToUnversionedUpdate(/*sorted*/ true);

        default:
            YT_ABORT();
    }
}

void TTableUploadOptions::Persist(NPhoenix::TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, UpdateMode);
    Persist(context, LockMode);
    Persist(context, TableSchema);
    Persist(context, SchemaModification);
    Persist(context, SchemaMode);
    Persist(context, OptimizeFor);
    Persist(context, CompressionCodec);
    Persist(context, ErasureCodec);
    Persist(context, SecurityTags);
    Persist(context, PartiallySorted);
}

////////////////////////////////////////////////////////////////////////////////

static void ValidateKeyColumnsEqual(const TKeyColumns& keyColumns, const TTableSchema& schema)
{
    if (keyColumns != schema.GetKeyColumns()) {
        THROW_ERROR_EXCEPTION("YPath attribute \"sorted_by\" must be compatible with table schema for a \"strong\" schema mode")
            << TErrorAttribute("key_columns", keyColumns)
            << TErrorAttribute("table_schema", schema);
    }
}

static void ValidateAppendKeyColumns(const TKeyColumns& keyColumns, const TTableSchema& schema, i64 rowCount)
{
    ValidateKeyColumns(keyColumns);

    if (rowCount == 0) {
        return;
    }

    auto tableKeyColumns = schema.GetKeyColumns();
    bool areKeyColumnsCompatible = true;
    if (tableKeyColumns.size() < keyColumns.size()) {
        areKeyColumnsCompatible = false;
    } else {
        for (int i = 0; i < keyColumns.size(); ++i) {
            if (tableKeyColumns[i] != keyColumns[i]) {
                areKeyColumnsCompatible = false;
                break;
            }
        }
    }

    if (!areKeyColumnsCompatible) {
        THROW_ERROR_EXCEPTION("Key columns mismatch while trying to append sorted data into a non-empty table")
            << TErrorAttribute("append_key_columns", keyColumns)
            << TErrorAttribute("current_key_columns", tableKeyColumns);
    }
}

TTableUploadOptions GetTableUploadOptions(
    const TRichYPath& path,
    const IAttributeDictionary& cypressTableAttributes,
    i64 rowCount)
{
    auto schema = cypressTableAttributes.Get<TTableSchema>("schema");
    auto schemaMode = cypressTableAttributes.Get<ETableSchemaMode>("schema_mode");
    auto optimizeFor = cypressTableAttributes.Get<EOptimizeFor>("optimize_for", EOptimizeFor::Lookup);
    auto compressionCodec = cypressTableAttributes.Get<NCompression::ECodec>("compression_codec");
    auto erasureCodec = cypressTableAttributes.Get<NErasure::ECodec>("erasure_codec", NErasure::ECodec::None);
    auto dynamic = cypressTableAttributes.Get<bool>("dynamic");

    // Some ypath attributes are not compatible with attribute "schema".
    if (path.GetAppend() && path.GetSchema()) {
        THROW_ERROR_EXCEPTION("YPath attributes \"append\" and \"schema\" are not compatible")
                << TErrorAttribute("path", path);
    }

    if (!path.GetSortedBy().empty() && path.GetSchema()) {
        THROW_ERROR_EXCEPTION("YPath attributes \"sorted_by\" and \"schema\" are not compatible")
                << TErrorAttribute("path", path);
    }

    TTableUploadOptions result;
    if (path.GetAppend() && !path.GetSortedBy().empty() && (schemaMode == ETableSchemaMode::Strong)) {
        ValidateKeyColumnsEqual(path.GetSortedBy(), schema);

        result.LockMode = ELockMode::Exclusive;
        result.UpdateMode = EUpdateMode::Append;
        result.SchemaMode = ETableSchemaMode::Strong;
        result.TableSchema = schema;
    } else if (path.GetAppend() && !path.GetSortedBy().empty() && (schemaMode == ETableSchemaMode::Weak)) {
        // Old behaviour.
        ValidateAppendKeyColumns(path.GetSortedBy(), schema, rowCount);

        result.LockMode = ELockMode::Exclusive;
        result.UpdateMode = EUpdateMode::Append;
        result.SchemaMode = ETableSchemaMode::Weak;
        result.TableSchema = TTableSchema::FromKeyColumns(path.GetSortedBy());
    } else if (path.GetAppend() && path.GetSortedBy().empty() && (schemaMode == ETableSchemaMode::Strong)) {
        result.LockMode = (schema.IsSorted() && !dynamic) ? ELockMode::Exclusive : ELockMode::Shared;
        result.UpdateMode = EUpdateMode::Append;
        result.SchemaMode = ETableSchemaMode::Strong;
        result.TableSchema = schema;
    } else if (path.GetAppend() && path.GetSortedBy().empty() && (schemaMode == ETableSchemaMode::Weak)) {
        // Old behaviour - reset key columns if there were any.
        result.LockMode = ELockMode::Shared;
        result.UpdateMode = EUpdateMode::Append;
        result.SchemaMode = ETableSchemaMode::Weak;
        result.TableSchema = TTableSchema();
    } else if (!path.GetAppend() && !path.GetSortedBy().empty() && (schemaMode == ETableSchemaMode::Strong)) {
        ValidateKeyColumnsEqual(path.GetSortedBy(), schema);

        result.LockMode = ELockMode::Exclusive;
        result.UpdateMode = EUpdateMode::Overwrite;
        result.SchemaMode = ETableSchemaMode::Strong;
        result.TableSchema = schema;
    } else if (!path.GetAppend() && !path.GetSortedBy().empty() && (schemaMode == ETableSchemaMode::Weak)) {
        result.LockMode = ELockMode::Exclusive;
        result.UpdateMode = EUpdateMode::Overwrite;
        result.SchemaMode = ETableSchemaMode::Weak;
        result.TableSchema = TTableSchema::FromKeyColumns(path.GetSortedBy());
    } else if (!path.GetAppend() && path.GetSchema() && (schemaMode == ETableSchemaMode::Strong)) {
        result.LockMode = ELockMode::Exclusive;
        result.UpdateMode = EUpdateMode::Overwrite;
        result.SchemaMode = ETableSchemaMode::Strong;
        result.TableSchema = *path.GetSchema();
    } else if (!path.GetAppend() && path.GetSchema() && (schemaMode == ETableSchemaMode::Weak)) {
        // Change from Weak to Strong schema mode.
        result.LockMode = ELockMode::Exclusive;
        result.UpdateMode = EUpdateMode::Overwrite;
        result.SchemaMode = ETableSchemaMode::Strong;
        result.TableSchema = *path.GetSchema();
    } else if (!path.GetAppend() && path.GetSortedBy().empty() && (schemaMode == ETableSchemaMode::Strong)) {
        result.LockMode = ELockMode::Exclusive;
        result.UpdateMode = EUpdateMode::Overwrite;
        result.SchemaMode = ETableSchemaMode::Strong;
        result.TableSchema = schema;
    } else if (!path.GetAppend() && path.GetSortedBy().empty() && (schemaMode == ETableSchemaMode::Weak)) {
        result.LockMode = ELockMode::Exclusive;
        result.UpdateMode = EUpdateMode::Overwrite;
        result.SchemaMode = ETableSchemaMode::Weak;
        result.TableSchema = TTableSchema();
    } else {
        // Do not use YT_ABORT here, since this code is executed inside scheduler.
        THROW_ERROR_EXCEPTION("Failed to define upload parameters")
            << TErrorAttribute("path", path)
            << TErrorAttribute("schema_mode", schemaMode)
            << TErrorAttribute("schema", schema);
    }

    if (path.GetAppend() && path.GetOptimizeFor()) {
        THROW_ERROR_EXCEPTION("YPath attributes \"append\" and \"optimize_for\" are not compatible")
            << TErrorAttribute("path", path);
    }

    if (path.GetOptimizeFor()) {
        result.OptimizeFor = *path.GetOptimizeFor();
    } else {
        result.OptimizeFor = optimizeFor;
    }

    if (path.GetAppend() && path.GetCompressionCodec()) {
        THROW_ERROR_EXCEPTION("YPath attributes \"append\" and \"compression_codec\" are not compatible")
            << TErrorAttribute("path", path);
    }

    if (path.GetCompressionCodec()) {
        result.CompressionCodec = *path.GetCompressionCodec();
    } else {
        result.CompressionCodec = compressionCodec;
    }

    if (path.GetAppend() && path.GetErasureCodec()) {
        THROW_ERROR_EXCEPTION("YPath attributes \"append\" and \"erasure_codec\" are not compatible")
            << TErrorAttribute("path", path);
    }

    if (path.GetErasureCodec()) {
        result.ErasureCodec = *path.GetErasureCodec();
    } else {
        result.ErasureCodec = erasureCodec;
    }

    if (path.GetSchemaModification() == ETableSchemaModification::UnversionedUpdateUnsorted) {
        THROW_ERROR_EXCEPTION("YPath attribute \"schema_modification\" cannot have value %Qlv for output tables",
            path.GetSchemaModification())
            << TErrorAttribute("path", path);
    } else if (!dynamic && path.GetSchemaModification() != ETableSchemaModification::None) {
        THROW_ERROR_EXCEPTION("YPath attribute \"schema_modification\" can have value %Qlv only for dynamic tables",
            path.GetSchemaModification())
            << TErrorAttribute("path", path);
    }
    result.SchemaModification = path.GetSchemaModification();

    if (!dynamic && path.GetPartiallySorted()) {
        THROW_ERROR_EXCEPTION("YPath attribute \"partially_sorted\" can be set only for dynamic tables")
            << TErrorAttribute("path", path);
    }
    result.PartiallySorted = path.GetPartiallySorted();

    result.SecurityTags = path.GetSecurityTags();

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
