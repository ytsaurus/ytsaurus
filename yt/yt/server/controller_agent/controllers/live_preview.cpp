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

void TLivePreview::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Chunks_,
        .template Serializer<TSetSerializer<TDefaultSerializer, TUnsortedTag>>());
    PHOENIX_REGISTER_FIELD(2, NodeDirectory_);
    PHOENIX_REGISTER_FIELD(3, Schema_,
        .template Serializer<TDerefSerializer<>>());
    PHOENIX_REGISTER_FIELD(4, OperationId_);
    PHOENIX_REGISTER_FIELD(5, Name_);
    PHOENIX_REGISTER_FIELD(6, Path_);
    PHOENIX_REGISTER_FIELD(7, Logger,
        .SinceVersion(ESnapshotVersion::ValidateLivePreviewChunks)
        .WhenMissing([] (TThis* this_, auto& /*context*/) {
            this_->Logger = ControllerLogger().WithTag("OperationId: %v", this_->OperationId_);
        }));
    registrar.AfterLoad([] (TThis* this_, auto& /*context*/) {
        this_->ValidateChunks();
        this_->Initialize();
    });
}

PHOENIX_DEFINE_TYPE(TLivePreview);

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
