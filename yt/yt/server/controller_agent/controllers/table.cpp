#include "table.h"

#include <yt/yt/server/lib/controller_agent/serialize.h>

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/ytlib/table_client/config.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NYTree;
using namespace NTableClient;
using namespace NCypressClient;

////////////////////////////////////////////////////////////////////////////////

void TLivePreviewTableBase::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, LivePreviewTableId);
    if (context.GetVersion() >= ESnapshotVersion::NewLivePreview) {
        Persist(context, LivePreviewTableName);
    }
}

////////////////////////////////////////////////////////////////////////////////

bool TTableBase::IsFile() const
{
    return Type == EObjectType::File;
}

void TTableBase::Persist(const TPersistenceContext &context)
{
    TUserObject::Persist(context);

    using NYT::Persist;
    Persist<TNonNullableIntrusivePtrSerializer<>>(context, Schema);
    Persist(context, SchemaId);
}

////////////////////////////////////////////////////////////////////////////////

bool TInputTable::IsForeign() const
{
    return Path.GetForeign();
}

bool TInputTable::IsPrimary() const
{
    return !IsForeign();
}

bool TInputTable::UseReadViaExecNode() const
{
    return Path.GetReadViaExecNode();
}

bool TInputTable::SupportsTeleportation() const
{
    if (Dynamic || !ColumnRenameDescriptors.empty()) {
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

void TInputTable::Persist(const TPersistenceContext& context)
{
    TTableBase::Persist(context);

    using NYT::Persist;
    Persist(context, Chunks);
    Persist(context, Comparator);
    Persist(context, SchemaMode);
    Persist(context, Dynamic);
}

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
        !Path.GetOutputTimestamp();
}

bool TOutputTable::IsDebugTable() const
{
    return OutputType == EOutputTableType::Stderr ||
        OutputType == EOutputTableType::Core;
}

void TOutputTable::Persist(const TPersistenceContext& context)
{
    TTableBase::Persist(context);
    TLivePreviewTableBase::Persist(context);

    using NYT::Persist;
    Persist(context, TableUploadOptions);
    Persist(context, TableWriterOptions);
    Persist(context, OutputType);
    if (context.GetVersion() >= ESnapshotVersion::AddChunkSchemas) {
        Persist(context, OriginalTableSchemaRevision);
    }
    Persist(context, Type);
    Persist(context, DataStatistics);
    // NB: Scheduler snapshots need not be stable.
    Persist(context, OutputChunkTreeIds);
    Persist(context, EffectiveAcl);
    Persist(context, WriterConfig);
    Persist(context, Dynamic);
    Persist(context, PivotKeys);
    Persist(context, TabletChunkListIds);
    Persist(context, OutputChunks);
    Persist(context, TableIndex);
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

////////////////////////////////////////////////////////////////////////////////

void TIntermediateTable::Persist(const TPersistenceContext& context)
{
    TLivePreviewTableBase::Persist(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
