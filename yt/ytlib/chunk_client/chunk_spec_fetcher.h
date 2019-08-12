#include "public.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>

#include <yt/client/chunk_client/read_limit.h>

#include <yt/core/misc/common.h>

#pragma once

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! Fetches chunk specs from master, waits for the result and processes the responses (possibly locating
//! foreign chunks).
class TChunkSpecFetcher
    : public TRefCounted
{
public:
    DEFINE_BYREF_RW_PROPERTY(std::vector<NProto::TChunkSpec>, ChunkSpecs);

public:
    // NB: invoker should be serialized.
    TChunkSpecFetcher(
        const NApi::NNative::IClientPtr& client,
        const NNodeTrackerClient::TNodeDirectoryPtr& nodeDirectory,
        const IInvokerPtr& invoker,
        int maxChunksPerFetch,
        int maxChunksPerLocateRequest,
        const std::function<void(const TChunkOwnerYPathProxy::TReqFetchPtr&)>& initializeFetchRequest,
        const NLogging::TLogger& logger,
        bool skipUnavialableChunks = false);

    void Add(
        NObjectClient::TObjectId objectId,
        NObjectClient::TCellTag externalCellTag,
        i64 chunkCount,
        const std::vector<TReadRange>& ranges = {TReadRange()});

    TFuture<void> Fetch();

private:
    NApi::NNative::IClientPtr Client_;
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;
    IInvokerPtr Invoker_;
    int MaxChunksPerFetch_;
    int MaxChunksPerLocateRequest_;
    std::function<void(const TChunkOwnerYPathProxy::TReqFetchPtr&)> InitializeFetchRequest_;
    NLogging::TLogger Logger;
    bool SkipUnavailableChunks_;
    std::vector<NProto::TChunkSpec*> ForeignChunkSpecs_;
    int TableIndex_ = 0;

    struct TCellState
    {
        NObjectClient::TObjectServiceProxy::TReqExecuteBatchPtr BatchReq;
        int ReqCount = 0;
        std::vector<int> TableIndices;
        std::vector<int> RangeIndices;
        std::vector<NProto::TChunkSpec> ChunkSpecs;
    };

    THashMap<NObjectClient::TCellTag, TCellState> CellTagToState_;

    TCellState& GetCellState(NObjectClient::TCellTag cellTag);

    void DoFetch();

    void DoFetchFromCell(NObjectClient::TCellTag cellTag);
};

DEFINE_REFCOUNTED_TYPE(TChunkSpecFetcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
