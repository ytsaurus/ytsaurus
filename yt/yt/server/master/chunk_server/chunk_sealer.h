#pragma once

#include "private.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/library/profiling/producer.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkSealer
    : public TRefCounted
{
public:
    TChunkSealer(
        TChunkManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap,
        TJobTrackerPtr jobTracker);
    ~TChunkSealer();

    void Start(TChunk* frontJournalChunk, int journalChunkCount);
    void Stop();

    bool IsEnabled();

    void ScheduleSeal(TChunk* chunk);

    void OnChunkDestroyed(NChunkServer::TChunk* chunk);

    void OnProfiling(NProfiling::TSensorBuffer* buffer) const;

    void ScheduleJobs(
        TNode* node,
        NNodeTrackerClient::NProto::TNodeResources* resourceUsage,
        const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
        std::vector<TJobPtr>* jobsToStart);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TChunkSealer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
