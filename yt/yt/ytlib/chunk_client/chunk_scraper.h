#pragma once

#include "public.h"

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TScrapedChunkInfo
{
    TChunkId ChunkId;
    TChunkReplicaList Replicas;
    EChunkAvailability Availability;
};

void FormatValue(TStringBuilderBase* builder, const TScrapedChunkInfo& info, TStringBuf spec);
std::ostream& operator<<(std::ostream& out, const TScrapedChunkInfo& info);

static constexpr TChunkScraperAvailabilityPolicy MetadataAvailablePolicy = TMetadataAvailablePolicy{};

//! A chunk scraper for unavailable chunks.
/*
 * Thread affinity: explained below.
 * A scraper does its work within `serializedInvoker`. A scraper is not thread-safe.
 * This implies that any interaction with a scraper must be syncronized with any
 * task running within `serializedInvoker`.
 * In general, it is safe to call any method of a scraper within an invoker `I`
 * if there is a serialized invoker `P` such that `P.CheckAffinity(serializedInvoker)`
 * and `P.CheckAffinity(I)` are both `true`, i.e. `P` is a common "superinvoker" of `serializedInvoker` and `I`.
 * In particular, it is safe to interact with a scraper within `serializedInvoker`.
 * If `P` is the controller's serialized cancelable invoker and `P.CheckAffinity(serializedInvoker)` is `true`,
 * then thread affinity of the scraper is `P`.
 */
class TChunkScraper
    : public TRefCounted
{
public:
    TChunkScraper(
        TChunkScraperConfigPtr config,
        IInvokerPtr serializedInvoker,
        IInvokerPtr heavyInvoker,
        TThrottlerManagerPtr throttlerManager,
        NApi::NNative::IClientPtr client,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        TChunkBatchLocatedHandler onChunkBatchLocated,
        TChunkScraperAvailabilityPolicy availabilityPolicy,
        NLogging::TLogger logger);

    ~TChunkScraper() override;

    //! Starts periodic polling.
    /*!
     *  Should be called when operation preparation is complete.
     *  Safe to call multiple times.
     */
    void Start();

    //! Stops periodic polling.
    void Stop();

    //! Adds `chunk` into scraper's queue.
    void Add(TChunkId chunk);

    //! Equivalent to `Add` with subsequent `OnChunkBecameUnavailable`.
    void AddUnavailable(TChunkId chunk);

    void Remove(TChunkId chunk);
    void OnChunkBecameUnavailable(TChunkId chunk);

private:
    const TChunkScraperConfigPtr Config_;
    const IInvokerPtr SerializedInvoker_;
    const IInvokerPtr HeavyInvoker_;
    const TThrottlerManagerPtr ThrottlerManager_;
    const NApi::NNative::IClientPtr Client_;
    const NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;
    const TChunkBatchLocatedHandler OnChunkBatchLocated_;
    const TChunkScraperAvailabilityPolicy AvailabilityPolicy_;
    const NLogging::TLogger Logger;

    class TScraperTask;
    THashMap<NObjectClient::TCellTag, TIntrusivePtr<TScraperTask>> ScraperTasks_;
    bool IsStarted_ = false;

    TScraperTask& GetTaskForChunk(TChunkId chunkId);
};

DEFINE_REFCOUNTED_TYPE(TChunkScraper)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
