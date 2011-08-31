#include "chunk_replication.h"

#include "../misc/serialize.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

// TODO: make configurable
static int MaxReplicationJobsPerHolder = 4;
static int MaxRemovalJobsPerHolder = 16;
static TDuration ChunkRefreshDelay = TDuration::Seconds(15);
static TDuration ChunkRefreshQuantum = TDuration::MilliSeconds(100);
static int MaxChunksPerRefresh = 1000;

static NLog::TLogger& Logger = ChunkManagerLogger;

////////////////////////////////////////////////////////////////////////////////

void TChunkReplication::RunJobControl(
    const THolder& holder,
    const yvector<NProto::TJobInfo>& runningJobs,
    yvector<NProto::TJobStartInfo>* jobsToStart,
    yvector<TJobId>* jobsToStop )
{
    int replicationJobCount;
    int removalJobCount;
    ProcessRunningJobs(
        holder,
        runningJobs,
        jobsToStop,
        &replicationJobCount,
        &removalJobCount);

    ScheduleReplicationJobs(
        holder,
        runningJobs,
        Max(0, MaxReplicationJobsPerHolder - replicationJobCount),
        jobsToStart);

    ScheduleRemovalJobs(
        holder,
        runningJobs,
        Max(0, MaxRemovalJobsPerHolder - removalJobCount),
        jobsToStart);
}

void TChunkReplication::ProcessRunningJobs(
    const THolder& holder,
    const yvector<NProto::TJobInfo>& runningJobs,
    yvector<TJobId>* jobsToStop,
    int* replicationJobCount,
    int* removalJobCount)
{
    *replicationJobCount = 0;
    *removalJobCount = 0;

    for (yvector<NProto::TJobInfo>::const_iterator it = runningJobs.begin();
        it != runningJobs.end();
        ++it)
    {
        const NProto::TJobInfo& jobInfo = *it;
        TJobId jobId = TJobId::FromProto(jobInfo.GetJobId());
        const TJob& job = ChunkManager->GetJob(jobId);
        EJobState jobState(jobInfo.GetState());
        switch (jobState) {
        case EJobState::Running:
            switch (job.Type) {
            case EJobType::Replicate:
                *replicationJobCount++;
                break;

            case EJobType::Remove:
                *removalJobCount++;
                break;

            default:
                YASSERT(false);
                break;
            }
            LOG_INFO("Job is running (JobId: %s, HolderId: %d)",
                ~jobId.ToString(),
                holder.Id);
            break;

        case EJobState::Completed:
            jobsToStop->push_back(jobId);
            LOG_INFO("Job is completed (JobId: %s, HolderId: %d)",
                ~jobId.ToString(),
                holder.Id);
            break;

        case EJobState::Failed:
            jobsToStop->push_back(jobId);
            LOG_WARNING("Job has failed (JobId: %s, HolderId: %d)",
                ~jobId.ToString(),
                holder.Id);
            break;

        default:
            YASSERT(false);
            break;
        }
    }
}

bool TChunkReplication::IsJobScheduled(
    const yvector<NProto::TJobInfo>& runningJobs,
    const TChunkId& chunkId)
{
    for (yvector<NProto::TJobInfo>::const_iterator it = runningJobs.begin();
        it != runningJobs.end();
        ++it)
    {
        const NProto::TJobInfo& jobInfo = *it;
        TJobId jobId = TJobId::FromProto(jobInfo.GetJobId());
        EJobState jobState(jobInfo.GetState());
        const TJob& job = ChunkManager->GetJob(jobId);
        if (jobState == EJobState::Running && job.ChunkId == chunkId) {
            return true;
        }
    }
    return false;
}

yvector<Stroka> TChunkReplication::GetTargetAddresses(
    const TChunk& chunk,
    int replicaCount)
{
    yhash_set<Stroka> forbiddenAddresses;

    const TChunk::TLocations& locations = chunk.Locations;
    for (TChunk::TLocations::const_iterator it = locations.begin();
        it != locations.end();
        ++it)
    {
        const THolder& holder = ChunkManager->GetHolder(*it);
        forbiddenAddresses.insert(holder.Address);
    }

    const TJobList* jobList = ChunkManager->FindJobList(chunk.Id);
    if (jobList != NULL) {
        const TJobList::TJobs& jobs = jobList->Jobs;
        for (TJobList::TJobs::const_iterator it = jobs.begin();
            it != jobs.end();
            ++it)
        {
            const TJob& job = ChunkManager->GetJob(*it);
            if (job.Type == EJobType::Replicate && job.ChunkId == chunk.Id) {
                forbiddenAddresses.insert(job.TargetAddresses.begin(), job.TargetAddresses.end());
            }
        }
    }

    yvector<THolderId> candidateHolders = ChunkPlacement->GetTargetHolders(
        replicaCount + forbiddenAddresses.size());

    yvector<Stroka> targetAddresses;
    for (yvector<THolderId>::const_iterator it = candidateHolders.begin();
        it != candidateHolders.end();
        ++it)
    {
        const THolder& holder = ChunkManager->GetHolder(*it);
        if (forbiddenAddresses.find(holder.Address) == forbiddenAddresses.end()) {
            targetAddresses.push_back(holder.Address);
        }
    }

    return targetAddresses;
}

void TChunkReplication::ScheduleReplicationJobs(
    const THolder& holder,
    const yvector<NProto::TJobInfo>& runningJobs,
    int maxJobsToStart,
    yvector<NProto::TJobStartInfo>* jobsToStart)
{
    const THolder::TChunkIds& chunkIds = holder.UnderreplicatedChunks;
    for (THolder::TChunkIds::iterator it = chunkIds.begin();
        it != chunkIds.end() && maxJobsToStart > 0;
        ++it)
    {
        const TChunkId& chunkId = *it;
        if (IsJobScheduled(runningJobs, chunkId))
            continue;

        const TChunk& chunk = ChunkManager->GetChunk(chunkId);

        int desiredCount = GetDesiredReplicaCount(chunk);

        int realCount;
        int plusCount;
        int minusCount;
        CountReplicas(chunk, &realCount, &plusCount, &minusCount);

        int additionalCount = desiredCount - (realCount + plusCount);
        if (additionalCount < 0)
            return;

        yvector<Stroka> targetAddresses = GetTargetAddresses(chunk, additionalCount);
        if (targetAddresses.empty())
            continue;

        TJobId jobId = TJobId::Create();

        NProto::TJobStartInfo startInfo;
        startInfo.SetJobId(jobId.ToProto());
        startInfo.SetType(EJobType::Replicate);
        startInfo.SetChunkId(chunkId.ToProto());
        ToProto(*startInfo.MutableTargetAddresses(), targetAddresses);
        jobsToStart->push_back(startInfo);
        --maxJobsToStart;

        LOG_INFO("Chunk replication scheduled (ChunkId: %s, HolderId: %d, JobId: %s, TargetAddresses: [%s])",
            ~chunkId.ToString(),
            holder.Id,
            ~jobId.ToString(),
            ~JoinStroku(targetAddresses, ", "));
    }
}

void TChunkReplication::ScheduleRemovalJobs(
    const THolder& holder,
    const yvector<NProto::TJobInfo>& runningJobs,
    int maxJobsToStart,
    yvector<NProto::TJobStartInfo>* jobsToStart)
{
    const THolder::TChunkIds& chunkIds = holder.OverreplicatedChunks;
    for (THolder::TChunkIds::iterator it = chunkIds.begin();
        it != chunkIds.end() && maxJobsToStart > 0;
        ++it)
    {
        const TChunkId& chunkId = *it;
        if (IsJobScheduled(runningJobs, chunkId))
            continue;

        TJobId jobId = TJobId::Create();

        NProto::TJobStartInfo startInfo;
        startInfo.SetJobId(jobId.ToProto());
        startInfo.SetType(EJobType::Remove);
        startInfo.SetChunkId(chunkId.ToProto());
        jobsToStart->push_back(startInfo);
        --maxJobsToStart;

        LOG_INFO("Chunk removal scheduled (ChunkId: %s, HolderId: %d, JobId: %s)",
            ~chunkId.ToString(),
            holder.Id,
            ~jobId.ToString());
    }
}

NYT::NChunkManager::EChunkState TChunkReplication::GetState(const TChunk& chunk)
{
    int actualReplicaCount = chunk.Locations.ysize();
    if (actualReplicaCount == 0) {
        return EChunkState::Lost;
    }

    int desiredCount = GetDesiredReplicaCount(chunk);

    int realCount;
    int plusCount;
    int minusCount;
    CountReplicas(chunk, &realCount, &plusCount, &minusCount);

    if (realCount - minusCount > desiredCount) {
        return EChunkState::Overreplicated;
    } else if (realCount + plusCount < desiredCount) {
        return EChunkState::Underreplicated;
    } else {
        return EChunkState::OK;
    }
}

// TODO: optimize?
void TChunkReplication::CountReplicas(
    const TChunk& chunk,
    int* real,
    int* plus,
    int* minus)
{
    const TChunk::TLocations& locations = chunk.Locations;
    *real = locations.ysize();

    yhash_set<Stroka> realAddresses(*real);
    for (TChunk::TLocations::const_iterator locationIt = locations.begin();
        locationIt != locations.end();
        ++locationIt)
    {
        const THolder& holder = ChunkManager->GetHolder(*locationIt);
        realAddresses.insert(holder.Address);
    }

    *plus = 0;
    *minus = 0;
    const TJobList* jobList = ChunkManager->FindJobList(chunk.Id);
    if (jobList != NULL) {
        for (TJobList::TJobs::const_iterator jobIt = jobList->Jobs.begin();
            jobIt != jobList->Jobs.end();
            ++jobIt)
        {
            const TJob& job = ChunkManager->GetJob(*jobIt);
            switch (job.Type) {
            case EJobType::Replicate:
                for (yvector<Stroka>::const_iterator targetIt = job.TargetAddresses.begin();
                    targetIt != job.TargetAddresses.end();
                    ++targetIt)
                {
                    if (realAddresses.find(*targetIt) == realAddresses.end()) {
                        *plus++;
                    }
                }
                break;

            case EJobType::Remove:
                if (realAddresses.find(job.RunnerAddress) != realAddresses.end()) {
                    *minus++;
                }
                break;

            default:
                YASSERT(false);
                break;
            }
        }
    }
}

int TChunkReplication::GetDesiredReplicaCount(const TChunk& chunk)
{
    // TODO: make configurable
    UNUSED(chunk);
    return 3;
}

void TChunkReplication::Refresh(TChunk& chunk)
{
    EChunkState state = GetState(chunk);
    UpdateState(chunk, state);

    LOG_INFO("Chunk refreshed (ChunkId: %s, State: %s)",
        ~chunk.Id.ToString(),
        ~state.ToString());
}

bool TChunkReplication::UpdateState(TChunk& chunk, EChunkState state)
{
    const TChunk::TLocations& locations = chunk.Locations;
    for (TChunk::TLocations::const_iterator it = locations.begin();
        it != locations.end();
        ++it)
    {
        THolder& holder = ChunkManager->GetHolderForUpdate(*it);

        switch (state) {
        case EChunkState::OK:
            holder.UnderreplicatedChunks.erase(chunk.Id);
            holder.OverreplicatedChunks.erase(chunk.Id);
            break;

        case EChunkState::Overreplicated:
            holder.UnderreplicatedChunks.erase(chunk.Id);
            holder.OverreplicatedChunks.insert(chunk.Id);
            break;

        case EChunkState::Underreplicated:
            holder.OverreplicatedChunks.erase(chunk.Id);
            holder.UnderreplicatedChunks.insert(chunk.Id);
            break;

        default:
            YASSERT(false);
            return false;
        }
    }
    return true;
}

void TChunkReplication::ScheduleRefresh(TChunk& chunk)
{
    if (RefreshSet.find(chunk.Id) != RefreshSet.end())
        return;

    TRefreshEntry entry;
    entry.ChunkId = chunk.Id;
    entry.When = TInstant::Now() + ChunkRefreshDelay;
    RefreshList.push_back(entry);
    RefreshSet.insert(chunk.Id);
}

void TChunkReplication::ScheduleNextRefresh()
{
    TDelayedInvoker::Get()->Submit(
        FromMethod(
            &TChunkReplication::OnRefresh,
            TPtr(this))
        ->Via(Invoker),
        ChunkRefreshQuantum);
}

void TChunkReplication::OnRefresh()
{
    TInstant now = TInstant::Now();
    for (int i = 0; i < MaxChunksPerRefresh; ++i) {
        if (RefreshList.empty())
            break;

        const TRefreshEntry& entry = RefreshList.front();
        if (entry.When > now)
            break;

        const TChunk* chunk = ChunkManager->FindChunk(entry.ChunkId);
        if (chunk != NULL) {
            // TODO: hack
            Refresh(const_cast<TChunk&>(*chunk));
        }

        YVERIFY(RefreshSet.erase(entry.ChunkId) == 1);
        RefreshList.pop_front();
    }
    ScheduleNextRefresh();
}

void TChunkReplication::StartRefresh( IInvoker::TPtr invoker )
{
    YASSERT(~Invoker == NULL);
    Invoker = invoker;
    ScheduleNextRefresh();
}

void TChunkReplication::StopRefresh()
{
    YASSERT(~Invoker != NULL);
    Invoker.Drop();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
