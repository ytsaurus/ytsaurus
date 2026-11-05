#pragma once

#include "private.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/api/client_common.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/core/misc/cache_config.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkSpecCache
    : public TRefCounted
{
public:
    struct TRequest
    {
        NObjectClient::TObjectId ObjectId;
        NObjectClient::TCellTag ExternalCellTag;
        i64 ChunkCount = 0;
        NHydra::TRevision MinContentRevision;
        //! Minimal acceptable value of the table's chunk merger revision (see #TTable::ChunkMergerRevision).
        i64 MinChunkMergerRevision = 0;
    };

    TChunkSpecCache(
        TSlruCacheConfigPtr config,
        int maxChunksPerFetch,
        int maxChunksPerLocateRequest,
        IInvokerPtr invoker,
        NLogging::TLogger logger,
        NProfiling::TProfiler profiler);
    ~TChunkSpecCache();

    TFuture<std::vector<TErrorOr<std::vector<NChunkClient::NProto::TChunkSpec>>>> GetChunkSpecs(
        std::vector<TRequest> requests,
        NApi::NNative::IClientPtr client,
        NApi::TMasterReadOptions masterReadOptions);

private:
    class TImpl;

    TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TChunkSpecCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
