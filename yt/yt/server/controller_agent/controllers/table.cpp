#include "table.h"

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>

#include <yt/yt/ytlib/controller_agent/serialize.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/ytlib/table_client/config.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NYTree;
using namespace NTableClient;
using namespace NCypressClient;

////////////////////////////////////////////////////////////////////////////////

void TLivePreviewTableBase::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, LivePreviewTableId)();
    PHOENIX_REGISTER_FIELD(2, LivePreviewTableName)();
}

PHOENIX_DEFINE_TYPE(TLivePreviewTableBase);

////////////////////////////////////////////////////////////////////////////////

bool TTableBase::IsFile() const
{
    return Type == EObjectType::File;
}

void TTableBase::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TUserObject>();

    PHOENIX_REGISTER_FIELD(1, Schema)
        .template Serializer<TNonNullableIntrusivePtrSerializer<>>()();
    PHOENIX_REGISTER_FIELD(2, SchemaId)();
}

PHOENIX_DEFINE_TYPE(TTableBase);

////////////////////////////////////////////////////////////////////////////////

bool TInputTable::IsForeign() const
{
    return Path.GetForeign();
}

bool TInputTable::IsPrimary() const
{
    return !IsForeign();
}

bool TInputTable::IsVersioned() const
{
    return Dynamic && Schema->IsSorted();
}

bool TInputTable::UseReadViaExecNode() const
{
    return Path.GetReadViaExecNode();
}

bool TInputTable::SupportsTeleportation() const
{
    if (Dynamic || !ColumnRenameDescriptors.empty() || !IsLocal(ClusterName)) {
        return false;
    }

    if (Schema->HasHunkColumns()) {
        return false;
    }

    auto pathColumnNames = Path.GetColumns();
    if (!pathColumnNames) {
        return true;
    }

    // Check if all columns are present in column filter and that schema is strict.

    if (pathColumnNames->size() != Schema->Columns().size()) {
        return false;
    }

    if (!Schema->GetStrict()) {
        return false;
    }

    for (const auto& pathColumnName : *pathColumnNames) {
        bool found = false;
        for (const auto& schemaColumn : Schema->Columns()) {
            if (pathColumnName == schemaColumn.Name()) {
                found = true;
                break;
            }
        }

        if (!found) {
            return false;
        }
    }

    return true;
}

void TInputTable::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TTableBase>();

    PHOENIX_REGISTER_FIELD(1, Chunks)();
    // COMPAT(alexelexa)
    PHOENIX_REGISTER_FIELD(2, HunkChunks)
        .SinceVersion(ESnapshotVersion::RemoteCopyDynamicTableWithHunks)();
    PHOENIX_REGISTER_FIELD(3, Comparator)();
    PHOENIX_REGISTER_FIELD(4, SchemaMode)();
    PHOENIX_REGISTER_FIELD(5, Dynamic)();
    // COMPAT(coteeq)
    PHOENIX_REGISTER_FIELD(6, ClusterName)
        .SinceVersion(ESnapshotVersion::RemoteInputForOperations)();
}

PHOENIX_DEFINE_TYPE(TInputTable);

////////////////////////////////////////////////////////////////////////////////

TOutputTable::TOutputTable(NYPath::TRichYPath path, EOutputTableType outputType)
    : TTableBase(std::move(path))
    , OutputType(outputType)
{ }

bool TOutputTable::IsBeginUploadCompleted() const
{
    return static_cast<bool>(UploadTransactionId);
}

bool TOutputTable::SupportsTeleportation() const
{
    return TableUploadOptions.SchemaModification == ETableSchemaModification::None &&
        Path.GetVersionedWriteOptions().WriteMode == EVersionedIOMode::Default &&
        !Path.GetOutputTimestamp();
}

bool TOutputTable::IsDebugTable() const
{
    return OutputType == EOutputTableType::Stderr ||
        OutputType == EOutputTableType::Core;
}

void TOutputTable::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TTableBase>();
    registrar.template BaseType<TLivePreviewTableBase>();

    PHOENIX_REGISTER_FIELD(1, TableUploadOptions)();
    PHOENIX_REGISTER_FIELD(2, TableWriterOptions)();
    PHOENIX_REGISTER_FIELD(3, OutputType)();
    registrar.template VirtualField<4>("OriginalTableSchemaRevision_", [] (TThis* /*this_*/, auto& context) {
        Load<NHydra::TRevision>(context);
    })
        .BeforeVersion(ESnapshotVersion::DropOriginalTableSchemaRevision)();
    PHOENIX_REGISTER_FIELD(5, Type)();
    PHOENIX_REGISTER_FIELD(6, DataStatistics)();
    // NB: Scheduler snapshots need not be stable.
    PHOENIX_REGISTER_FIELD(7, OutputChunkTreeIds)();
    // COMPAT(alexelexa)
    PHOENIX_REGISTER_FIELD(8, OutputHunkChunkListId)
        .SinceVersion(ESnapshotVersion::RemoteCopyDynamicTableWithHunks)();
    PHOENIX_REGISTER_FIELD(9, EffectiveAcl)();
    PHOENIX_REGISTER_FIELD(10, WriterConfig)();
    PHOENIX_REGISTER_FIELD(11, Dynamic)();
    PHOENIX_REGISTER_FIELD(12, PivotKeys)();
    PHOENIX_REGISTER_FIELD(13, TabletChunkListIds)();
    // COMPAT(alexelexa)
    PHOENIX_REGISTER_FIELD(14, TabletHunkChunkListIds)
        .SinceVersion(ESnapshotVersion::RemoteCopyDynamicTableWithHunks)();
    PHOENIX_REGISTER_FIELD(15, OutputChunks)();
    // COMPAT(alexelexa)
    PHOENIX_REGISTER_FIELD(16, OutputHunkChunks)
        .SinceVersion(ESnapshotVersion::RemoteCopyDynamicTableWithHunks)();
    PHOENIX_REGISTER_FIELD(17, TableIndex)();
}

TOutputStreamDescriptorPtr TOutputTable::GetStreamDescriptorTemplate(int tableIndex)
{
    auto descriptor = New<TOutputStreamDescriptor>();
    descriptor->TableUploadOptions = TableUploadOptions;
    descriptor->TableWriterOptions = CloneYsonStruct(TableWriterOptions);
    descriptor->TableWriterOptions->TableIndex = tableIndex;
    descriptor->TableWriterConfig = WriterConfig;
    descriptor->Timestamp = Timestamp;
    // Output tables never lose data (hopefully), so we do not need to store
    // recovery info for chunks that get there.
    descriptor->RequiresRecoveryInfo = false;
    descriptor->CellTags = {ExternalCellTag};
    descriptor->ImmediatelyUnstageChunkLists = false;
    descriptor->IsOutputTableDynamic = Dynamic;
    descriptor->PartitionTag = TableIndex;

    return descriptor;
}

PHOENIX_DEFINE_TYPE(TOutputTable);

////////////////////////////////////////////////////////////////////////////////

void TIntermediateTable::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TLivePreviewTableBase>();
}

PHOENIX_DEFINE_TYPE(TIntermediateTable);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
