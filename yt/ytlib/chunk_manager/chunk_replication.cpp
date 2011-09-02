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

////////////////////////////////////////////////////////////////////////////////

TChunkReplication::TChunkReplication(
    TChunkManager::TPtr chunkManager,
    TChunkPlacement::TPtr chunkPlacement)
    : ChunkManager(chunkManager)
    , ChunkPlacement(chunkPlacement)
{ }

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

    ScheduleJobs(
        holder,
        runningJobs,
        Max(0, MaxReplicationJobsPerHolder - replicationJobCount),
        Max(0, MaxRemovalJobsPerHolder - removalJobCount),
        jobsToStart);
}

void TChunkReplication::RegisterHolder(const THolder& holder)
{
    auto holderInfo = New<THolderInfo>();
    YVERIFY(HolderInfoMap.insert(MakePair(holder.Id, holderInfo)).Second());

    for (auto it = holder.Chunks.begin();
         it != holder.Chunks.end();
         ++it)
    {
        ScheduleRefresh(*it);
    }
}

void TChunkReplication::UnregisterHolder(const THolder& holder)
{
    YVERIFY(HolderInfoMap.erase(holder.Id) == 1);
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

    for (auto it = runningJobs.begin();
        it != runningJobs.end();
        ++it)
    {
        auto jobInfo = *it;
        auto jobId = TJobId::FromProto(jobInfo.GetJobId());
        auto job = ChunkManager->GetJob(jobId);
        auto jobState = EJobState(jobInfo.GetState());
        switch (jobState) {
        case EJobState::Running:
            switch (job.Type) {
                case EJobType::Replicate:
                    ++*replicationJobCount;
                    break;

                case EJobType::Remove:
                    ++*removalJobCount;
                    break;

                default:
                    YASSERT(false);
                    break;
            }
            LOG_INFO("Job running (JobId: %s, HolderId: %d)",
                ~jobId.ToString(),
                holder.Id);
            break;

        case EJobState::Completed:
            jobsToStop->push_back(jobId);
            LOG_INFO("Job completed (JobId: %s, HolderId: %d)",
                ~jobId.ToString(),
                holder.Id);
            break;

        case EJobState::Failed:
            jobsToStop->push_back(jobId);
            LOG_WARNING("Job failed (JobId: %s, HolderId: %d)",
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
    for (auto it = runningJobs.begin();
        it != runningJobs.end();
        ++it)
    {
        auto jobInfo = *it;
        auto jobId = TJobId::FromProto(jobInfo.GetJobId());
        EJobState jobState(jobInfo.GetState());
        auto job = ChunkManager->GetJob(jobId);
        if (jobState == EJobState::Running && job.ChunkId == chunkId) {
            return true;
        }
    }
    return false;
}

bool TChunkReplication::IsRefreshScheduled(const TChunkId& chunkId)
{
    return RefreshSet.find(chunkId) != RefreshSet.end();
}

yvector<Stroka> TChunkReplication::GetTargetAddresses(
    const TChunk& chunk,
    int replicaCount)
{
    yhash_set<Stroka> forbiddenAddresses;

    const TChunk::TLocations& locations = chunk.Locations;
    for (auto it = locations.begin();
        it != locations.end();
        ++it)
    {
        auto holder = ChunkManager->GetHolder(*it);
        forbiddenAddresses.insert(holder.Address);
    }

    auto jobList = ChunkManager->FindJobList(chunk.Id);
    if (jobList != NULL) {
        const TJobList::TJobs& jobs = jobList->Jobs;
        for (auto it = jobs.begin();
            it != jobs.end();
            ++it)
        {
            auto job = ChunkManager->GetJob(*it);
            if (job.Type == EJobType::Replicate && job.ChunkId == chunk.Id) {
                forbiddenAddresses.insert(job.TargetAddresses.begin(), job.TargetAddresses.end());
            }
        }
    }

    auto candidateHolders = ChunkPlacement->GetTargetHolders(replicaCount + forbiddenAddresses.size());

    yvector<Stroka> targetAddresses;
    for (auto it = candidateHolders.begin();
        it != candidateHolders.end() && targetAddresses.ysize() < replicaCount;
        ++it)
    {
        auto holder = ChunkManager->GetHolder(*it);
        if (forbiddenAddresses.find(holder.Address) == forbiddenAddresses.end()) {
            targetAddresses.push_back(holder.Address);
        }
    }

    return targetAddresses;
}

bool TChunkReplication::TryScheduleReplicationJob(
    const THolder& holder,
    const TChunkId& chunkId,
    const yvector<NProto::TJobInfo>& runningJobs,
    yvector<NProto::TJobStartInfo>* jobsToStart)
{
    auto chunk = ChunkManager->FindChunk(chunkId);
    if (IsJobScheduled(runningJobs, chunkId) ||
        IsRefreshScheduled(chunkId) ||
        chunk == NULL)
    {
        return false;
    }

    int desiredCount;
    int realCount;
    int plusCount;
    int minusCount;
    EChunkState state;
    GetReplicaStatistics(
        *chunk,
        &desiredCount,
        &realCount,
        &plusCount,
        &minusCount,
        &state);

    int additionalCount = desiredCount - (realCount + plusCount);
    if (additionalCount < 0)
        return false;

    auto targetAddresses = GetTargetAddresses(*chunk, additionalCount);
    if (targetAddresses.empty())
        return false;

    auto jobId = TJobId::Create();

    NProto::TJobStartInfo startInfo;
    startInfo.SetJobId(jobId.ToProto());
    startInfo.SetType(EJobType::Replicate);
    startInfo.SetChunkId(chunkId.ToProto());
    ToProto(*startInfo.MutableTargetAddresses(), targetAddresses);
    jobsToStart->push_back(startInfo);
    ScheduleRefresh(chunkId);

    LOG_INFO("Chunk replication scheduled (ChunkId: %s, HolderId: %d, JobId: %s, TargetAddresses: [%s])",
        ~chunkId.ToString(),
        holder.Id,
        ~jobId.ToString(),
        ~JoinStroku(targetAddresses, ", "));

    return true;
}


bool TChunkReplication::TryScheduleRemovalJob(
    const THolder& holder,
    const TChunkId& chunkId,
    const yvector<NProto::TJobInfo>& runningJobs,
    yvector<NProto::TJobStartInfo>* jobsToStart )
{
    auto chunk = ChunkManager->FindChunk(chunkId);
    if (IsJobScheduled(runningJobs, chunkId) ||
        IsRefreshScheduled(chunkId) ||
        chunk == NULL)
    {
        return false;
    }

    auto jobId = TJobId::Create();

    NProto::TJobStartInfo startInfo;
    startInfo.SetJobId(jobId.ToProto());
    startInfo.SetType(EJobType::Remove);
    startInfo.SetChunkId(chunkId.ToProto());
    jobsToStart->push_back(startInfo);
    ScheduleRefresh(chunkId);

    LOG_INFO("Chunk removal scheduled (ChunkId: %s, HolderId: %d, JobId: %s)",
        ~chunkId.ToString(),
        holder.Id,
        ~jobId.ToString());

    return true;
}

void TChunkReplication::ScheduleJobs(
    const THolder& holder,
    const yvector<NProto::TJobInfo>& runningJobs,
    int maxReplicationJobsToStart,
    int maxRemovalJobsToStart,
    yvector<NProto::TJobStartInfo>* jobsToStart)
{
    auto holderInfo = FindHolderInfo(holder.Id);
    if (~holderInfo == NULL)
        return;

    THolderInfo::TChunkIds& underreplicatedChunks = holderInfo->UnderreplicatedChunks;
    yvector<TChunkId> underreplicatedChunksToSweep;
    for (auto it = underreplicatedChunks.begin();
        it != underreplicatedChunks.end() && maxReplicationJobsToStart > 0;
        ++it)
    {
        auto chunkId = *it;
        if (TryScheduleReplicationJob(holder, chunkId, runningJobs, jobsToStart)) {
            --maxReplicationJobsToStart;
        } else {
            underreplicatedChunksToSweep.push_back(chunkId);
        }
    }

    for (auto it = underreplicatedChunksToSweep.begin();
         it != underreplicatedChunksToSweep.end();
         ++it)
    {
        underreplicatedChunks.erase(*it);
    }

    THolderInfo::TChunkIds& overreplicatedChunks = holderInfo->OverreplicatedChunks;
    yvector<TChunkId> overreplicatedChunksToSweep;
    for (auto it = overreplicatedChunks.begin();
        it != overreplicatedChunks.end() && maxRemovalJobsToStart > 0;
        ++it)
    {
        auto chunkId = *it;
        if (TryScheduleRemovalJob(holder, chunkId, runningJobs, jobsToStart)) {
            --maxRemovalJobsToStart;
        } else {
            overreplicatedChunksToSweep.push_back(chunkId);
        }
    }

    for (auto it = overreplicatedChunksToSweep.begin();
        it != overreplicatedChunksToSweep.end();
        ++it)
    {
        overreplicatedChunks.erase(*it);
    }
}

void TChunkReplication::GetReplicaStatistics(
    const TChunk& chunk,
    int* desiredCount,
    int* realCount,
    int* plusCount,
    int* minusCount,
    EChunkState* state)
{
    const TChunk::TLocations& locations = chunk.Locations;

    *desiredCount = GetDesiredReplicaCount(chunk);
    *realCount = locations.ysize();
    *plusCount = 0;
    *minusCount = 0;

    if (*realCount == 0) {
        *state = EChunkState::Lost;
        return;
    }

    auto jobList = ChunkManager->FindJobList(chunk.Id);
    if (jobList != NULL) {
        yhash_set<Stroka> realAddresses(*realCount);
        for (TChunk::TLocations::const_iterator locationIt = locations.begin();
            locationIt != locations.end();
            ++locationIt)
        {
            auto holder = ChunkManager->GetHolder(*locationIt);
            realAddresses.insert(holder.Address);
        }

        for (auto jobIt = jobList->Jobs.begin();
            jobIt != jobList->Jobs.end();
            ++jobIt)
        {
            auto job = ChunkManager->GetJob(*jobIt);
            switch (job.Type) {
            case EJobType::Replicate:
                for (auto targetIt = job.TargetAddresses.begin();
                    targetIt != job.TargetAddresses.end();
                    ++targetIt)
                {
                    if (realAddresses.find(*targetIt) == realAddresses.end()) {
                        ++*plusCount;
                    }
                }
                break;

            case EJobType::Remove:
                if (realAddresses.find(job.RunnerAddress) != realAddresses.end()) {
                    ++*minusCount;
                }
                break;

            default:
                YASSERT(false);
                break;
            }
        }
    }

    if (*realCount - *minusCount > *desiredCount) {
        *state = EChunkState::Overreplicated;
    } else if (*realCount + *plusCount < *desiredCount) {
        *state = EChunkState::Underreplicated;
    } else {
        *state = EChunkState::OK;
    }
}

int TChunkReplication::GetDesiredReplicaCount(const TChunk& chunk)
{
    // TODO: make configurable
    UNUSED(chunk);
    return 3;
}

void TChunkReplication::Refresh(const TChunk& chunk)
{
    int desiredCount;
    int realCount;
    int plusCount;
    int minusCount;
    EChunkState state;
    GetReplicaStatistics(
        chunk,
        &desiredCount,
        &realCount,
        &plusCount,
        &minusCount,
        &state);

    const TChunk::TLocations& locations = chunk.Locations;
    for (auto it = locations.begin();
        it != locations.end();
        ++it)
    {
        auto holderInfo = FindHolderInfo(*it);
        if (~holderInfo == NULL)
            continue;

        switch (state) {
            case EChunkState::OK:
                holderInfo->UnderreplicatedChunks.erase(chunk.Id);
                holderInfo->OverreplicatedChunks.erase(chunk.Id);
                break;

            case EChunkState::Overreplicated:
                holderInfo->UnderreplicatedChunks.erase(chunk.Id);
                holderInfo->OverreplicatedChunks.insert(chunk.Id);
                break;

            case EChunkState::Underreplicated:
                holderInfo->OverreplicatedChunks.erase(chunk.Id);
                holderInfo->UnderreplicatedChunks.insert(chunk.Id);
                break;

            default:
                YASSERT(false);
                break;
        }
    }

    LOG_INFO("Chunk refreshed (ChunkId: %s, ReplicaCount: %d+%d-%d, DesiredReplicaCount: %d, State: %s)",
        ~chunk.Id.ToString(),
        realCount,
        plusCount,
        minusCount,
        desiredCount,
        ~state.ToString());
}

void TChunkReplication::ScheduleRefresh(const TChunkId& chunkId)
{
    if (RefreshSet.find(chunkId) != RefreshSet.end())
        return;

    TRefreshEntry entry;
    entry.ChunkId = chunkId;
    entry.When = TInstant::Now() + ChunkRefreshDelay;
    RefreshList.push_back(entry);
    RefreshSet.insert(chunkId);
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
    auto now = TInstant::Now();
    for (int i = 0; i < MaxChunksPerRefresh; ++i) {
        if (RefreshList.empty())
            break;

        auto entry = RefreshList.front();
        if (entry.When > now)
            break;

        auto chunk = ChunkManager->FindChunk(entry.ChunkId);
        if (chunk != NULL) {
            Refresh(*chunk);
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

TChunkReplication::THolderInfo::TPtr
TChunkReplication::FindHolderInfo(THolderId holderId)
{
    auto it = HolderInfoMap.find(holderId);
    return it == HolderInfoMap.end() ? NULL : it->Second();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
