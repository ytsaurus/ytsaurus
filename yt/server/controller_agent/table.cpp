#include "table.h"

#include "serialize.h"

#include <yt/ytlib/chunk_client/input_chunk.h>

namespace NYT {
namespace NControllerAgent {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TLivePreviewTableBase::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    using NYT::Load;

    if (context.IsLoad() && context.GetVersion() < 202152) {
        Load<std::vector<NCypressClient::TNodeId>>(context.LoadContext());
    }
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

void TInputTable::Persist(const TPersistenceContext& context)
{
    TUserObject::Persist(context);

    using NYT::Persist;
    Persist(context, ChunkCount);
    Persist(context, Chunks);
    Persist(context, Schema);
    Persist(context, SchemaMode);
    Persist(context, IsDynamic);
}

////////////////////////////////////////////////////////////////////////////////

bool TOutputTable::IsBeginUploadCompleted() const
{
    return static_cast<bool>(UploadTransactionId);
}

void TOutputTable::Persist(const TPersistenceContext& context)
{
    TUserObject::Persist(context);
    TLivePreviewTableBase::Persist(context);

    using NYT::Persist;
    Persist(context, TableUploadOptions);
    Persist(context, Options);
    Persist(context, ChunkPropertiesUpdateNeeded);
    Persist(context, OutputType);
    Persist(context, Type);
    Persist(context, DataStatistics);
    // NB: Scheduler snapshots need not be stable.
    Persist(context, OutputChunkTreeIds);
    Persist(context, EffectiveAcl);
    Persist(context, WriterConfig);
}

TEdgeDescriptor TOutputTable::GetEdgeDescriptorTemplate()
{
    TEdgeDescriptor descriptor;
    descriptor.DestinationPool = nullptr;
    descriptor.TableUploadOptions = TableUploadOptions;
    descriptor.TableWriterOptions = CloneYsonSerializable(Options);
    descriptor.TableWriterConfig = WriterConfig;
    descriptor.Timestamp = Timestamp;
    // Output tables never lose data (hopefully), so we do not need to store
    // recovery info for chunks that get there.
    descriptor.RequiresRecoveryInfo = false;
    descriptor.CellTag = CellTag;
    descriptor.ImmediatelyUnstageChunkLists = false;
    return descriptor;
}

////////////////////////////////////////////////////////////////////////////////

void TIntermediateTable::Persist(const TPersistenceContext& context)
{
    TLivePreviewTableBase::Persist(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
