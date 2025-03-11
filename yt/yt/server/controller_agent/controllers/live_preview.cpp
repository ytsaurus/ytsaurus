#include "live_preview.h"

#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/ytlib/controller_agent/serialize.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NChunkClient;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TLivePreview::TLivePreview(
    TTableSchemaPtr schema,
    TNodeDirectoryPtr nodeDirectory,
    TLogger logger,
    TOperationId operationId,
    TString name,
    TYPath path)
    : Schema_(std::move(schema))
    , NodeDirectory_(std::move(nodeDirectory))
    , OperationId_(operationId)
    , Name_(std::move(name))
    , Path_(std::move(path))
    , Logger(std::move(logger))
{
    Initialize();
}

TError TLivePreview::TryInsertChunk(TInputChunkPtr chunk)
{
    if (Schema_->IsSorted() && !chunk->BoundaryKeys()) {
        return TError("Missing boundary keys in a chunk of a sorted live preview table")
            << NYT::TErrorAttribute("path", Path_)
            << NYT::TErrorAttribute("chunk_id", chunk->GetChunkId());
    }

    InsertOrCrash(Chunks_, std::move(chunk));

    return {};
}

TError TLivePreview::TryEraseChunk(const TInputChunkPtr& chunk)
{
    if (!Chunks_.erase(chunk)) {
        return TError("Erasing non present chunk of a live preview table")
            << NYT::TErrorAttribute("path", Path_)
            << NYT::TErrorAttribute("chunk_id", chunk->GetChunkId());
    }

    return {};
}

void TLivePreview::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist<TSetSerializer<TDefaultSerializer, TUnsortedTag>>(context, Chunks_);
    Persist(context, NodeDirectory_);
    Persist(context, *Schema_);
    Persist(context, OperationId_);
    Persist(context, Name_);
    Persist(context, Path_);

    if (context.GetVersion() >= ESnapshotVersion::ValidateLivePreviewChunks) {
        Persist(context, Logger);
    } else {
        Logger = ControllerLogger().WithTag("OperationId: %v", OperationId_);
    }

    if (context.IsLoad()) {
        ValidateChunks();
        Initialize();
    }
}

void TLivePreview::Initialize()
{
    Service_ = New<TVirtualStaticTable>(Chunks_, Schema_, NodeDirectory_, OperationId_, Name_, Path_);
}

void TLivePreview::ValidateChunks()
{
    THashSet<TInputChunkPtr> chunks;
    std::swap(chunks, Chunks_);

    for (auto chunk : chunks) {
        auto error = TryInsertChunk(chunk);
        YT_LOG_ALERT_UNLESS(
            error.IsOK(),
            error,
            "Error validating a chunk in a live preview (ChunkId: %v, Name: %v, Path: %v)",
            chunk->GetChunkId(),
            Name_,
            Path_);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
