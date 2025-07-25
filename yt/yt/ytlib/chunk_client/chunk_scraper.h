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
    bool Missing = false;
};

using TChunkBatchLocatedHandler = TCallback<void(std::vector<TScrapedChunkInfo>)>;

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
        const THashSet<TChunkId>& chunkIds,
        TChunkBatchLocatedHandler onChunkBatchLocated,
        NLogging::TLogger logger);

    ~TChunkScraper();

    //! Starts periodic polling.
    /*!
     *  Should be called when operation preparation is complete.
     *  Safe to call multiple times.
     */
    void Start();

    //! Stops periodic polling.
    void Stop();

private:
    const TChunkScraperConfigPtr Config_;
    const IInvokerPtr SerializedInvoker_;
    const IInvokerPtr HeavyInvoker_;
    const TThrottlerManagerPtr ThrottlerManager_;
    const NApi::NNative::IClientPtr Client_;
    const NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;
    const TChunkBatchLocatedHandler OnChunkBatchLocated_;
    const NLogging::TLogger Logger;

    struct TScraperTaskWrapper;
    std::vector<TScraperTaskWrapper> ScraperTasks_;

    //! Create scraper tasks for each cell.
    void CreateTasks(const THashSet<TChunkId>& chunkIds);
};

DEFINE_REFCOUNTED_TYPE(TChunkScraper)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
