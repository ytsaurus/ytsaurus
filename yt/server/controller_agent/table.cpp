#include "table.h"

#include "serialize.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

void TLivePreviewTableBase::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, LivePreviewTableId);
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

////////////////////////////////////////////////////////////////////////////////

void TIntermediateTable::Persist(const TPersistenceContext& context)
{
    TLivePreviewTableBase::Persist(context);
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT