#include "global_worker_manager.h"
#include "literal_requests_hard_reserve.h"
#include "workers_storage.h"

#include <contrib/ydb/library/yql/providers/dq/actors/events/events.h>
#include <contrib/ydb/library/yql/providers/dq/common/attrs.h>
#include <yt/yql/providers/dq/scheduler/dq_scheduler.h>

#include <contrib/ydb/library/yql/providers/dq/worker_manager/worker_manager_common.h>
#include <contrib/ydb/library/yql/providers/dq/actors/actor_helpers.h>
#include <contrib/ydb/library/yql/providers/dq/actors/execution_helpers.h>
#include <contrib/ydb/library/yql/providers/dq/api/grpc/api.grpc.pb.h>
#include <contrib/ydb/library/yql/providers/dq/counters/counters.h>

#include <yql/essentials/utils/failure_injector/failure_injector.h>

#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/log/log.h>

#include <contrib/ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>
#include <library/cpp/protobuf/util/pb_io.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/svnversion/svnversion.h>

#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/generic/guid.h>
#include <util/system/fs.h>
#include <util/system/getpid.h>
#include <util/generic/scope.h>
#include <util/random/shuffle.h>

#include <algorithm>

namespace NYql {

using namespace NActors;
using namespace NDqs;
using namespace NThreading;
using namespace NMonitoring;
using EFileType = Yql::DqsProto::TFile::EFileType;
using TFileResource = Yql::DqsProto::TFile;

namespace {

size_t ComputeTargetCapacity(const TVector<TResourceManagerOptions>& resourceUploaderOptions)
{
    size_t targetCapacity = 0;
    for (const auto& options : resourceUploaderOptions) {
        const auto configuredWorkerCapacity = options.YtBackend.GetWorkerCapacity();
        YQL_ENSURE(configuredWorkerCapacity >= 0,
            "Negative DQ worker capacity " << configuredWorkerCapacity
                << " configured for cluster " << options.YtBackend.GetClusterName());

        const auto jobs = static_cast<size_t>(options.YtBackend.GetMaxJobs());
        const auto workerCapacity = static_cast<size_t>(Max(configuredWorkerCapacity, 1));
        const auto backendCapacity = jobs * workerCapacity;
        YQL_ENSURE(targetCapacity <= std::numeric_limits<size_t>::max() - backendCapacity,
            "DQ target capacity overflow while adding cluster " << options.YtBackend.GetClusterName());
        targetCapacity += backendCapacity;
    }

    return targetCapacity;
}

} // namespace

union TDqResourceId {
    struct {
        ui32 Counter;
        ui32 Epoch;
    };
    ui64 Data;
};

static_assert(sizeof(TDqResourceId) == 8);

class TWorkerStopFilter {
public:
    static constexpr TDuration MaxStopFilterDuration = TDuration::Minutes(10);

    TWorkerStopFilter(const Yql::DqsProto::JobStopRequest& request, TInstant now)
        : Revision(request.GetRevision())
        , ClusterName(request.GetClusterName())
        , WorkerId(GetGuid(request.GetWorkerId()))
        , NegativeRevision(request.GetNegativeRevision())
        , LastUpdate(now)
    {
        for (const auto& attr : request.GetAttribute()) {
            Attributes.emplace(attr.GetKey(), attr.GetValue());
        }

        if (!request.GetForce() && request.GetTtlSeconds() != 0) {
            if (request.GetTtlSeconds() < 0) {
                Valid = false;
            } else {
                Deadline = now + Min(
                    TDuration::Seconds(request.GetTtlSeconds()),
                    MaxStopFilterDuration);
            }
        }
    }

    bool IsValid() const {
        return Valid;
    }

    bool Match(const TWorkerInfo& workerInfo, TInstant now) const {
        if (IsExpired(now)) {
            return false;
        }
        return MatchInternal(workerInfo, now);
    }

    bool IsExpired(TInstant now) const {
        if (Deadline) {
            return now >= *Deadline;
        }
        return now - LastUpdate > MaxStopFilterDuration;
    }

private:
    bool MatchInternal(const TWorkerInfo& workerInfo, TInstant now) const {
        if (!Revision.empty() && NegativeRevision == (Revision == workerInfo.Revision)) {
            return false;
        }
        if (!ClusterName.empty() && ClusterName != workerInfo.ClusterName) {
            return false;
        }
        if (!WorkerId.IsEmpty() && WorkerId != workerInfo.WorkerId) {
            return false;
        }

        for (const auto& [k, v] : Attributes) {
            auto i = workerInfo.Attributes.find(k);
            if (i == workerInfo.Attributes.end()) {
                return false;
            }
            if (i->second != v) {
                return false;
            }
        }

        LastUpdate = now;

        return true;
    }

    const TString Revision;
    const TString ClusterName;
    const TGUID WorkerId;
    const bool NegativeRevision;
    THashMap<TString, TString> Attributes;

    mutable TInstant LastUpdate;
    TMaybe<TInstant> Deadline;
    bool Valid = true;
};

// used for fast state recovery on restarted jobs
class TLocalCriticalFiles: public IFileCache {
public:
// used in IsReady
// exe unsupported
    void AddFile(const TString& path, const TString& objectId) override {
        Y_UNUSED(path);
        TGuard<TMutex> guard(Mutex);
        ObjectIds.insert(objectId);
    }

    void Clear() override {
        TGuard<TMutex> guard(Mutex);
        ObjectIds.clear();
    }

    TMaybe<TString> FindFile(const TString& objectId) override {
        Y_UNUSED(objectId);
        return { };
    }

    TMaybe<TString> AcquireFile(const TString& objectId) override {
        Y_UNUSED(objectId);
        return { };
    }

    void ReleaseFile(const TString& objectId) override {
        Y_UNUSED(objectId);
        return;
    }

    bool Contains(const TString& objectId) override {
        Y_UNUSED(objectId);
        return false;
    }

    void Walk(const std::function<void(const TString& objectId)>& f) override {
        TGuard<TMutex> guard(Mutex);
        for (const auto& id : ObjectIds) {
            f(id);
        }
    }

    i64 FreeDiskSize() override {
        return 0;
    }

    ui64 UsedDiskSize() override {
        return 0;
    }

    TString GetDir() override {
        return ""; // unused
    }

private:
    TMutex Mutex;
    THashSet<TString> ObjectIds;
};


// used in gwm master
class TAllCriticalFiles {
public:
    template<typename VectorLike>
    void Add(
        const TGUID& serviceNodeId,
        ui32 nodeId,
        const TString& revision,
        const TString& address,
        ui32 pid,
        const TString& startTime,
        const VectorLike& files)
    {
        auto now = TInstant::Now();
        auto& t = PerServiceNode[serviceNodeId];
        t.LastUpdate = now;
        t.NodeId = nodeId;
        t.Revision = revision;
        t.Pid = pid;
        t.Address = address;
        if (!startTime.empty()) {
            t.StartTime = startTime;
        } else if (t.StartTime.empty()) {
            t.StartTime = ToString(now);
        }
        bool needUpdate = false;
        for (const auto& f : files) {
            needUpdate |= t.Files.insert(f.GetObjectId()).second;
        }

        if (static_cast<int>(t.Files.size()) > static_cast<int>(files.size())) {
            needUpdate = true;
            THashSet<TString> hash;
            for (const auto& f : files) {
                hash.insert(f.GetObjectId());
            }
            THashSet<TString> toDrop;
            for (const auto& f : t.Files) {
                if (!hash.contains(f)) {
                    toDrop.insert(f);
                }
            }
            for (const auto& f : toDrop) {
                t.Files.erase(f);
            }
        }

        if (needUpdate) {
            t.OrderedFiles.clear();
            for (const auto& f : files) {
                t.OrderedFiles.push_back(f.GetObjectId());
            }
        }

        needUpdate |= ClearOld(now);

        if (needUpdate) {
            Rebuild();
        }
    }

    const TVector<TWorkerInfo::TFileResource>& GetResources() const {
        return Resources;
    }

    struct TServiceNodeFiles {
        ui32 NodeId;
        THashSet<TString> Files;
        TVector<TString> OrderedFiles;
        TInstant LastUpdate = TInstant::Now();
        TString Revision;
        ui32 Pid;
        TString Address;
        TString StartTime;
    };

    const THashMap<TGUID, TServiceNodeFiles>& GetNodes() const {
        return PerServiceNode;
    }

private:
    bool ClearOld(TInstant now) {
        THashSet<TGUID> toDrop;
        for (const auto& [k, v] : PerServiceNode) {
            if (k.IsEmpty()) { // local node
                continue;
            }

            if (now - v.LastUpdate > TDuration::Seconds(10)) {
                toDrop.insert(k);
            }
        }

        for (const auto& k : toDrop) {
            PerServiceNode.erase(k);
        }

        return !toDrop.empty();
    }

    void Rebuild() {
        Resources.clear();
        THashSet<TString> uniq;
        THashSet<TString> uniqExe;
        for (const auto& [_, v] : PerServiceNode) {
            for (int i = 0; i < static_cast<int>(v.OrderedFiles.size()) - 1; ++i) {
                const auto& f = v.OrderedFiles[i];
                uniq.insert(f);
            }
            if (!v.Files.empty()) {
                uniqExe.insert(v.OrderedFiles.back());
            }
        }
        for (const auto& f : uniq) {
            TWorkerInfo::TFileResource resource;
            resource.SetObjectType(TWorkerInfo::TFileResource::EUDF_FILE);
            resource.SetObjectId(f);
            Resources.push_back(resource);
        }
        //exe upload controlled via DqControl
        //for (const auto& f : uniqExe) {
        //    TWorkerInfo::TFileResource resource;
        //    resource.SetName("dq_vanilla_job.lite");
        //    resource.SetObjectType(TWorkerInfo::TFileResource::EEXE_FILE);
        //    resource.SetObjectId(f);
        //    Resources.push_back(resource);
        //}
    }

    THashMap<TGUID, TServiceNodeFiles> PerServiceNode; // serviceNodeId -> List
    TVector<TWorkerInfo::TFileResource> Resources;
};

struct TLiteralHardReserveValues {
    ui64 Capacity = 0;
    ui64 FreeSlots = 0;
    ui64 EffectiveReserve = 0;
    ui64 CurrentEpochLargeSlots = 0;
    ui64 LargeLimit = 0;
    ui64 LargeHeadroom = 0;
    ui64 LargeBudget = 0;
    ui64 Deficit = 0;

    static TLiteralHardReserveValues Calculate(
        ui64 capacity,
        ui64 freeSlots,
        ui64 effectiveReserve,
        ui64 currentEpochLargeSlots)
    {
        TLiteralHardReserveValues values;
        values.Capacity = capacity;
        values.FreeSlots = freeSlots;
        values.EffectiveReserve = effectiveReserve;
        values.CurrentEpochLargeSlots = currentEpochLargeSlots;
        values.LargeLimit = capacity - effectiveReserve;
        values.LargeHeadroom = currentEpochLargeSlots < values.LargeLimit
            ? values.LargeLimit - currentEpochLargeSlots
            : 0;
        values.LargeBudget = Min(freeSlots, values.LargeHeadroom);
        values.Deficit = currentEpochLargeSlots > values.LargeLimit
            ? currentEpochLargeSlots - values.LargeLimit
            : 0;
        return values;
    }
};

struct TLiteralHardReserveState {
    struct TCluster {
        ui32 ConfiguredPercent = 0;
        ui64 ConfiguredMinSlots = 0;
        TLiteralHardReserveValues Values;
    };

    NDq::IScheduler::TProcessLimits ProcessLimits;
    THashMap<TString, TCluster> Clusters;
    TWorkersStorage::TLargeSlotsBudgetByCluster LargeSlotsBudgetByCluster;
};

struct TLiteralHardReserveCounters {
    struct TValueCounters {
        const TDynamicCounters::TCounterPtr EffectiveSlots;
        const TDynamicCounters::TCounterPtr ProtectedFreeSlots;
        const TDynamicCounters::TCounterPtr CurrentEpochLargeSlots;
        const TDynamicCounters::TCounterPtr LargeSlotsLimit;
        const TDynamicCounters::TCounterPtr LargeSlotsHeadroom;
        const TDynamicCounters::TCounterPtr Deficit;

        explicit TValueCounters(const TSensorsGroupPtr& group)
            : EffectiveSlots(group->GetCounter("LiteralHardReserveEffectiveSlots"))
            , ProtectedFreeSlots(group->GetCounter("LiteralHardReserveProtectedFreeSlots"))
            , CurrentEpochLargeSlots(group->GetCounter("LiteralHardReserveCurrentEpochLargeSlots"))
            , LargeSlotsLimit(group->GetCounter("LiteralHardReserveLargeSlotsLimit"))
            , LargeSlotsHeadroom(group->GetCounter("LiteralHardReserveLargeSlotsHeadroom"))
            , Deficit(group->GetCounter("LiteralHardReserveDeficit"))
        { }

        void Update(const TLiteralHardReserveValues& values) const {
            *EffectiveSlots = values.EffectiveReserve;
            *ProtectedFreeSlots = values.FreeSlots - values.LargeBudget;
            *CurrentEpochLargeSlots = values.CurrentEpochLargeSlots;
            *LargeSlotsLimit = values.LargeLimit;
            *LargeSlotsHeadroom = values.LargeHeadroom;
            *Deficit = values.Deficit;
        }
    };

    struct TClusterCounters {
        const TDynamicCounters::TCounterPtr ConfiguredPercent;
        const TDynamicCounters::TCounterPtr ConfiguredMinSlots;
        const TValueCounters Values;

        explicit TClusterCounters(const TSensorsGroupPtr& group)
            : ConfiguredPercent(group->GetCounter("LiteralHardReserveConfiguredPercent"))
            , ConfiguredMinSlots(group->GetCounter("LiteralHardReserveConfiguredMinSlots"))
            , Values(group)
        { }

        void Update(const TLiteralHardReserveState::TCluster& state) const {
            *ConfiguredPercent = state.ConfiguredPercent;
            *ConfiguredMinSlots = state.ConfiguredMinSlots;
            Values.Update(state.Values);
        }
    };

    THashMap<TString, TClusterCounters> Clusters;
    const TDynamicCounters::TCounterPtr ConfiguredClusters;
    const TValueCounters Total;

    TLiteralHardReserveCounters(
        const TSensorsGroupPtr& group,
        const TLiteralRequestsHardReserveSettings& settings)
        : ConfiguredClusters(group->GetCounter("LiteralHardReserveConfiguredClusters"))
        , Total(group)
    {
        for (const auto& [clusterName, _] : settings.GetClusters()) {
            Clusters.emplace(
                clusterName,
                TClusterCounters(group->GetSubgroup("ClusterName", clusterName)));
        }
    }

    void Update(const TLiteralHardReserveState& state) const {
        *ConfiguredClusters = state.Clusters.size();

        TLiteralHardReserveValues aggregate;
        for (const auto& [clusterName, cluster] : state.Clusters) {
            Clusters.at(clusterName).Update(cluster);
            const auto& values = cluster.Values;
            aggregate.Capacity += values.Capacity;
            aggregate.FreeSlots += values.FreeSlots;
            aggregate.EffectiveReserve += values.EffectiveReserve;
            aggregate.CurrentEpochLargeSlots += values.CurrentEpochLargeSlots;
            aggregate.LargeLimit += values.LargeLimit;
            aggregate.LargeHeadroom += values.LargeHeadroom;
            aggregate.LargeBudget += values.LargeBudget;
            aggregate.Deficit += values.Deficit;
        }
        Total.Update(aggregate);
    }
};

class TGlobalWorkerManager: public TWorkerManagerCommon<TGlobalWorkerManager> {
public:

    static constexpr char ActorName[] = "GWM";

    explicit TGlobalWorkerManager(
           const ICoordinationHelper::TPtr& coordinator,
           const TVector<TResourceManagerOptions>& resourceUploaderOptions,
           IMetricsRegistryPtr metricsRegistry,
           const NProto::TDqConfig::TScheduler& schedulerConfig,
           TDuration scheduleInterval,
           TGlobalWorkerManagerActorIdOptions actorIdOptions)
       : TWorkerManagerCommon<TGlobalWorkerManager>(&TGlobalWorkerManager::Initialization)
        , Coordinator(coordinator)
        , LeaderResolver(std::make_shared<TSingleNodeResolver>())
        , Metrics(metricsRegistry->GetSensors()->GetSubgroup("counters", "gwm"))
        , UploaderMetrics(metricsRegistry->GetSensors()->GetSubgroup("counters", "uploader"))
        , LatencyHistogram(Metrics->GetHistogram("LeaderLatency", ExponentialHistogram(10, 2, 1)))
        , AllocateWorkersWithExeFileCounter(
            Metrics->GetSubgroup("component", "requests")
                ->GetCounter("AllocateWorkersWithExeFile", /*derivative=*/true))
        , AllocateWorkersWithoutExeFileCounter(
            Metrics->GetSubgroup("component", "requests")
                ->GetCounter("AllocateWorkersWithoutExeFile", /*derivative=*/true))
        , OverloadedTotal(Metrics->GetSubgroup("component", "overload")
            ->GetCounter("Total", /*derivative=*/ true))
        , RejectedNoWorkers(Metrics->GetSubgroup("component", "overload")
            ->GetCounter("NoWorkers", /*derivative=*/ true))
        , RejectedAfterWorkersLost(Metrics->GetSubgroup("component", "overload")
            ->GetCounter("WorkersLost", /*derivative=*/ true))
        , LiteralHardReserve(resourceUploaderOptions)
        , Workers(
            Coordinator->GetNodeId(),
            Metrics,
            metricsRegistry->GetSensors()->GetSubgroup("counters", "workers"),
            LiteralHardReserve.IsEnabled())
        , HardReserveCounters(Metrics->GetSubgroup("component", "scheduler"), LiteralHardReserve)
        , Scheduler(NDq::IScheduler::Make(
            schedulerConfig,
            metricsRegistry,
            schedulerConfig.GetLimitRunningTasksPerUserPercent() > 0
                ? ComputeTargetCapacity(resourceUploaderOptions)
                : 0))
        , Revision(ToString(GetProgramCommitId()))
        , ResourceUploaderOptions(resourceUploaderOptions)
        , ActorIdOptions(actorIdOptions)
        , WaitListSize(nullptr)
        , ScheduleInterval(scheduleInterval)
    { }

private:

#define HHFunc(TEvType, HandleFunc)                                                 \
    case TEvType::EventType: {                                                      \
        Y_SCOPE_EXIT(&) { UpdateMetrics(/*updateRunningLimitedQueueSize*/ false); };\
        typename TEvType::TPtr* x = reinterpret_cast<typename TEvType::TPtr*>(&ev); \
        TString name(#TEvType);                                                     \
        name = name.substr(name.find_last_of(':')+1);                               \
        *Metrics->GetSubgroup("component", "requests")->GetCounter(name, true) += 1;\
        TInstant t = TInstant::Now();                                               \
        HandleFunc(*x, TActivationContext::ActorContextFor(this->SelfId()));        \
        if (CurrentStateFunc() == &TGlobalWorkerManager::Leader) {                  \
            LatencyHistogram->Collect((TInstant::Now() - t).MilliSeconds());        \
        }                                                                           \
        break;                                                                      \
    }

    STRICT_STFUNC(Initialization, {
        HHFunc(TEvBecomeFollower, StartFollower)
        HHFunc(TEvBecomeLeader, StartLeader)
        CFunc(TEvents::TEvBootstrap::EventType, Bootstrap)

        HHFunc(TEvAllocateWorkersRequest, OnAllocateWorkersRequestStub)
        HHFunc(TEvFreeWorkersNotify, OnFreeWorkersStub)
        HHFunc(TEvRegisterNode, OnRegisterNodeStub)
        HHFunc(TEvClusterStatus, OnClusterStatusStub)
        HHFunc(TEvQueryStatus, OnQueryStatusStub)
        HHFunc(TEvIsReady, OnIsReadyStub)
        HHFunc(TEvGetMasterRequest, OnGetMasterStub)
        HHFunc(TEvConfigureFailureInjectorRequest, OnConfigureFailureInjectorStub)
        HHFunc(TEvOperationStop, OnOperationStopStub)
        HHFunc(TEvJobStop, OnJobStopStub)

        cFunc(TEvents::TEvPoison::EventType, PassAway)
        HHFunc(TEvRoutesRequest, this->OnRoutesRequest)
    })

    STRICT_STFUNC(Leader, {
        HHFunc(TEvAllocateWorkersRequest, OnAllocateWorkersRequest)
        HHFunc(TEvFreeWorkersNotify, OnFreeWorkers)
        HHFunc(TEvRegisterNode, OnRegisterNode)

        HHFunc(TEvBecomeLeader, StartLeader)
        HHFunc(TEvBecomeFollower, StartFollower)

        IgnoreFunc(TEvInterconnect::TEvNodeConnected)

        HHFunc(TEvents::TEvUndelivered, OnUndelivered)
        HHFunc(TEvInterconnect::TEvNodeDisconnected, OnExecuterDisconnected)

        cFunc(TEvents::TEvPoison::EventType, PassAway)

        HHFunc(TEvUploadComplete, OnFileUploaded)
        HHFunc(TEvClusterStatus, OnClusterStatus)
        HHFunc(TEvQueryStatus, OnQueryStatus)
        HHFunc(TEvIsReady, OnIsReady)
        HHFunc(TEvJobStop, OnJobStop)
        HHFunc(TEvGetMasterRequest, OnGetMaster)
        HHFunc(TEvConfigureFailureInjectorRequest, OnConfigureFailureInjector)
        cFunc(TEvTick::EventType, OnTick)
        HHFunc(TEvRoutesRequest, OnRoutesRequest)
        HHFunc(TEvOperationStop, OnOperationStop)
    })

#define FORWARD(T) \
    case T::EventType: { \
        /*YQL_CLOG(TRACE, ProviderDq) << "Forward event " << etype << " to " << LeaderId;*/ \
        typename T::TPtr& x = *(reinterpret_cast<typename T::TPtr*>(&ev)); \
        if (!x->Get()->Record.GetIsForwarded()) { \
            x->Get()->Record.SetIsForwarded(true); \
            Send(x->Forward(GetGlobalWorkerManagerActorId(LeaderId))); \
        } \
        break; \
    }

    STRICT_STFUNC(Follower, {
        HHFunc(TEvBecomeLeader, StartLeader)
        HHFunc(TEvBecomeFollower, StartFollower)
        // Notifications from the previous leader's sessions can already be queued.
        HHFunc(TEvInterconnect::TEvNodeConnected, OnExecuterConnectedStub)
        HHFunc(TEvInterconnect::TEvNodeDisconnected, OnExecuterDisconnected)
        HHFunc(TEvents::TEvUndelivered, OnUndelivered)
        HHFunc(TEvAllocateWorkersRequest, StartUploadAndForward)
        FORWARD(TEvFreeWorkersNotify)
        FORWARD(TEvRegisterNode)
        FORWARD(TEvJobStop)
        FORWARD(TEvClusterStatus)
        FORWARD(TEvQueryStatus)
        FORWARD(TEvOperationStop)
        HHFunc(TEvIsReady, OnIsReadyForward)
        HHFunc(TEvGetMasterRequest, OnGetMaster)
        cFunc(TEvents::TEvPoison::EventType, PassAway)
        HHFunc(TEvUploadComplete, OnFileUploaded)
        IgnoreFunc(TEvTick)
        HHFunc(TEvRoutesRequest, this->OnRoutesRequest)
    })

#undef FORWARD
#undef HHFunc

    void Tick() {
        Schedule(ScheduleInterval, new TEvTick);
    }

    void OnTick() {
        auto now = TInstant::Now();
        if (now - LastCleanTime > CleanInterval) {
            Y_SCOPE_EXIT(&) { UpdateMetrics(/*updateRunningLimitedQueueSize*/ true); };
            CleanUp(now);
            LastCleanTime = now;
        }
        TryResume();
        Tick();
    }

    void DoPassAway() override {
        Y_SCOPE_EXIT(&) { UpdateMetrics(/*updateRunningLimitedQueueSize*/ true); };
        for (const auto& sender: Scheduler->Cleanup()) {
            Send(sender, new TEvAllocateWorkersResponse("Shutdown in progress", NYql::NDqProto::StatusIds::UNAVAILABLE));
        }
    }

    static TString FormatFile(const TFileResource& file) {
        static constexpr const char* FileTypes[] = {"UDF", "USER", "EXE"};
        const int typeIdx = static_cast<int>(file.GetObjectType());
        const TString typeName = (typeIdx >= 0 && typeIdx < static_cast<int>(std::size(FileTypes))) ? FileTypes[typeIdx] : ToString(typeIdx);
        return TStringBuilder()
            << "name=" << file.GetName()
            << " objectId=" << file.GetObjectId()
            << " localPath=" << (file.GetLocalPath().empty() ? "<empty>" : file.GetLocalPath())
            << " type=" << typeName
            << " size=" << file.GetSize();
    }

    TMaybe<TString> CheckFiles(const TVector<TFileResource>& files) {
        YQL_CLOG(DEBUG, ProviderDq) << "CheckFiles: total=" << files.size();
        for (const auto& file : files) {
            YQL_CLOG(DEBUG, ProviderDq) << "CheckFiles: " << FormatFile(file);
            if (file.GetObjectType() == Yql::DqsProto::TFile::EEXE_FILE) {
                if (file.GetName().empty()) {
                    return "Unnamed exe file " + FormatFile(file);
                }
            } else {
                if (file.GetLocalPath().empty()) {
                    YQL_CLOG(DEBUG, ProviderDq) << "CheckFiles: empty localPath, skipping existence check for " << FormatFile(file);
                } else if (!NFs::Exists(file.GetLocalPath())) {
                    YQL_CLOG(WARN, ProviderDq) << "CheckFiles: file not found at localPath=" << file.GetLocalPath()
                        << " (this path is likely the client's local path, not the server's)";
                    return "Unknown file: " + FormatFile(file);
                }
            }

            if (file.GetObjectId().empty()) {
                return "Empty objectId (md5, revision) for " + FormatFile(file);
            }
        }

        return {};
    }

    std::pair<bool, TString> MaybeUploadUnsafe(bool isForwarded, const TVector<TFileResource>& files, bool useCache = false) {
        const bool hasExeFile = AnyOf(files, [] (const TFileResource& file) {
            return file.GetObjectType() == Yql::DqsProto::TFile::EEXE_FILE;
        });

        YQL_CLOG(TRACE, ProviderDq) << "MaybeUpload: isForwarded=" << isForwarded
            << " files=" << files.size()
            << " uploaderOptions=" << ResourceUploaderOptions.size();
        for (const auto& file : files) {
            YQL_CLOG(TRACE, ProviderDq) << "MaybeUpload file: " << FormatFile(file);
        }

        if (isForwarded) {
            // Upload/validation was already performed by the previous GWM hop,
            // but the executable information is still present in the request.
            YQL_CLOG(DEBUG, ProviderDq) << "MaybeUpload: skipping (isForwarded=true)";
            return std::make_pair(hasExeFile, "");
        }

        auto error = CheckFiles(files);

        if (error) {
            return std::make_pair(false, *error);
        }

        if (ResourceUploaderOptions.empty()) {
            return std::make_pair(hasExeFile, "");
        }

        TVector<TResourceFile> preparedFiles;
        TVector<TString> preparedFilesIds;
        TVector<TResourceFile> preparedExeFiles;
        auto now = TInstant::Now();
        auto cacheTimeout = TDuration::Hours(1);

        if (useCache) {
            THashSet<TString> toremove;
            for (const auto& [objectId, ts] : LastUploadCache) {
                if (now - ts > cacheTimeout) {
                    toremove.insert(objectId);
                }
            }
            for (const auto& objectId : toremove) {
                LastUploadCache.erase(objectId);
            }
        }

        for (const auto& file : files) {
            if (Uploading.contains(file.GetObjectId())) {
                continue;
            }

            // exe files can be without local path
            if (file.GetLocalPath().empty()) {
                continue;
            }
            auto fileName = file.GetName();
            auto objectId = file.GetObjectId();
            auto objectType = file.GetObjectType();
            auto localPath = file.GetLocalPath();
            auto size = file.GetSize();

            if (useCache || objectType == Yql::DqsProto::TFile_EFileType_EEXE_FILE) {
                if (LastUploadCache.contains(objectId) && (now - LastUploadCache[objectId] < cacheTimeout)) {
                    continue;
                }
                LastUploadCache[objectId] = now;
            }

            TResourceFile preparedFile;
            preparedFile.ObjectId = objectId;
            preparedFile.LocalFileName = fileName;
            preparedFile.ContentMd5 = file.GetContentMd5();
            preparedFile.Attributes["file_name"] = preparedFile.GetRemoteFileName();
            preparedFile.File = ::TFile(localPath, RdOnly | OpenExisting);
            preparedFile.RemoteFileName = objectId;
            preparedFiles.push_back(preparedFile);
            preparedFilesIds.push_back(objectId);

            YQL_CLOG(DEBUG, ProviderDq) << "Start upload: " << fileName << " objectId=" << objectId << " size=" << size
                << " clusters=" << ResourceUploaderOptions.size();

            int uploadProcesses = static_cast<int>(ResourceUploaderOptions.size());

            if (objectType == Yql::DqsProto::TFile_EFileType_EEXE_FILE) {
                // COMPAT (aozeritsky)
                preparedFile.RemoteFileName = fileName;
                uploadProcesses *= 2;

                for (auto options : ResourceUploaderOptions) {
                    options.Counters = UploaderMetrics;
                    options.LockName = objectId + "." + options.YtBackend.GetClusterName();
                    options.Files = { preparedFile };
                    options.UploadPrefix = options.UploadPrefix + "/bin/" + objectId;
                    auto actor = CreateResourceUploader(options, Coordinator);
                    UploadProcesses[RegisterChild(actor)].push_back(objectId);
                }
            }

            Uploading.emplace(objectId, TUploadingInfo {objectType, fileName, uploadProcesses, size});
        }

        Shuffle(preparedFiles.begin(), preparedFiles.end(), TReallyFastRng32(TInstant::Now().NanoSeconds()));

        if (!preparedFiles.empty()) {
            TStringStream clusterList;
            for (size_t i = 0; i < ResourceUploaderOptions.size(); ++i) {
                if (i > 0) {
                    clusterList << ", ";
                }
                clusterList << ResourceUploaderOptions[i].YtBackend.GetClusterName();
            }
            YQL_CLOG(DEBUG, ProviderDq) << "Starting UDF upload: files=" << preparedFiles.size()
                << " clusters=" << ResourceUploaderOptions.size() << " [" << clusterList.Str() << "]";
        }

        for (auto options : ResourceUploaderOptions) {
            options.Counters = UploaderMetrics;
            options.LockName.clear();

            if (!preparedFiles.empty()) {
                options.Files = preparedFiles;
                options.UploadPrefix = options.UploadPrefix + "/udfs";
                auto actor = CreateResourceUploader(options, Coordinator);
                UploadProcesses.emplace(RegisterChild(actor), preparedFilesIds);
            }
        }

        return std::make_pair(hasExeFile, "");
    }

    std::pair<bool, TString> MaybeUpload(bool isForwarded, const TVector<TFileResource>& files, bool useCache = false) {
        try {
            return MaybeUploadUnsafe(isForwarded, files, useCache);
        } catch (...) {
            return {false, CurrentExceptionMessage()};
        }
    }


    void StartUploadAndForward(TEvAllocateWorkersRequest::TPtr& ev, const TActorContext& ctx)
    {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(ev->Get()->Record.GetTraceId());
        auto [hasExeFile, err]  = MaybeUpload(ev->Get()->Record.GetIsForwarded(), TVector<TFileResource>(ev->Get()->Record.GetFiles().begin(), ev->Get()->Record.GetFiles().end()));
        if (!err.empty()) {
            YQL_CLOG(INFO, ProviderDq) << "StartUploadAndForward, error " << err;
            Send(ev->Sender, new TEvAllocateWorkersResponse(err, NYql::NDqProto::StatusIds::EXTERNAL_ERROR));
            return;
        }

        if (hasExeFile) {
            *AllocateWorkersWithExeFileCounter += 1;
        } else {
            *AllocateWorkersWithoutExeFileCounter += 1;
        }

        if (!hasExeFile && LeaderRevision != Revision) {
            YQL_CLOG(INFO, ProviderDq) << "StartUploadAndForward, Wrong revision " << LeaderRevision << ", " << Revision;
            Send(ev->Sender, new TEvAllocateWorkersResponse(
                Sprintf("Wrong revision %s!=%s", LeaderRevision.c_str(), Revision.c_str()), NYql::NDqProto::StatusIds::UNSUPPORTED));
        } else {
            ev->Get()->Record.SetIsForwarded(true);
            ctx.Send(ev->Forward(GetGlobalWorkerManagerActorId(LeaderId)));
        }
    }

    void OnFileUploaded(TEvUploadComplete::TPtr& ev, const TActorContext& ctx)
    {
        Y_UNUSED(ctx);
        auto sender = ev->Sender;
        auto it = UploadProcesses.find(sender);
        if (it == UploadProcesses.end()) {
            return;
        }
        UnregisterChild(sender);

        for (const auto& objectId : it->second) {
            auto jt = Uploading.find(objectId);
            Y_ABORT_UNLESS(jt != Uploading.end());
            Workers.AddResource(objectId, jt->second.ObjectType, jt->second.ObjectName, jt->second.Size);
            if (--jt->second.Clusters <= 0) {
                // Refresh cache timestamp on completion so the TTL starts from upload finish,
                // not from when the upload was initiated (which may have been long ago).
                LastUploadCache[objectId] = TInstant::Now();
                Uploading.erase(jt);
            }
        }
        UploadProcesses.erase(it);
    }

    void EnterTerminatingMode() {
        if (TerminatingMode) {
            return;
        }
        TerminatingMode = true;
        LocalCriticalFiles->Clear();
        YQL_CLOG(INFO, ProviderDq) << "Graceful shutdown: release lock and stop leader election";
        if (LockId) {
            Send(LockId, new TEvBecomeFollower());
        }
    }

    void TransitionToFollowerOfRemoteLeader() {
        UnsubscribeAll();
        WaitListSize = nullptr;
        Workers.Clear();
        for (const auto& [key, value] : AllocatedResources) {
            Send(value.ActorId, new TEvents::TEvPoison());
        }
        for (const auto sender : Scheduler->Cleanup()) {
            Send(sender, new TEvAllocateWorkersResponse("Worker reallocation is required because of DQ leader change", NYql::NDqProto::StatusIds::UNAVAILABLE));
        }
        AllocatedResources.clear();
        CurrentEpochLargeSlotsByCluster.clear();
        if (LiteralHardReserve.IsEnabled()) {
            YQL_CLOG(INFO, ProviderDq)
                << "Literal hard reserve accounting reset; tasks of previous leaders are not classified, epoch="
                << LeaderEpoch;
        }
        for (auto& [k, v] : LiteralQueries) {
            *v -= *v;
        }
        Become(&TGlobalWorkerManager::Follower);
    }

    void StartLeader(TEvBecomeLeader::TPtr& ev, const TActorContext& ctx)
    {
        Y_UNUSED(ctx);
        YQL_CLOG(INFO, ProviderDq) << "GWM received BecomeLeader epoch=" << ev->Get()->LeaderEpoch;
        if (TerminatingMode) {
            YQL_CLOG(INFO, ProviderDq) << "Skip Leader request in terminating mode";
            return;
        }
        LeaderEpoch = CurrentResourceId.Epoch = ev->Get()->LeaderEpoch;
        CurrentResourceId.Counter = 0;

        // update leader info (used for leader)
        auto attributes = NYT::NodeFromYsonString(ev->Get()->Attributes).AsMap();
        UpdateLeaderInfo(attributes);
        // Report the leader even before its first IsReady request.
        UpdateLocalCriticalFiles();

        YQL_CLOG(INFO, ProviderDq) << "Become leader, epoch=" << CurrentResourceId.Epoch;
        YQL_CLOG(INFO, ProviderDq) << "Leader resolver=" << LeaderHost << ":" << LeaderPort;

        if (LeaderPinger) {
            UnregisterChild(LeaderPinger);
            LeaderPinger = TActorId();
        }
        Become(&TGlobalWorkerManager::Leader);
        Tick();
    }

    void StartFollower(TEvBecomeFollower::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        YQL_CLOG(INFO, ProviderDq) << "GWM received BecomeFollower cookie=" << ev->Cookie;
        if (ev->Cookie == 1) {
            EnterTerminatingMode();
            return;
        }
        auto attributes = NYT::NodeFromYsonString(ev->Get()->Attributes).AsMap();
        UpdateRemoteLeaderInfo(attributes);
        if (TerminatingMode && LeaderId == SelfId().NodeId()) {
            YQL_CLOG(INFO, ProviderDq) << "Terminating mode: lock still points to us, keep local workers";
            return;
        }
        if (!LeaderPinger) {
            TResourceManagerOptions rmOptions;
            rmOptions.FileCache = LocalCriticalFiles;
            LeaderPinger = RegisterChild(Coordinator->CreateServiceNodePinger(
                LeaderResolver,
                rmOptions));
        }

        YQL_CLOG(TRACE, ProviderDq) << " Following leader: " << LeaderId;
        YQL_CLOG(INFO, ProviderDq) << "Leader resolver=" << LeaderHost << ":" << LeaderPort;

        TransitionToFollowerOfRemoteLeader();
    }

    void UpdateLeaderInfo(const THashMap<TString, NYT::TNode>& attributes) {
        LeaderHost = attributes.at(NCommonAttrs::HOSTNAME_ATTR).AsString();
        LeaderPort = std::stoi(attributes.at(NCommonAttrs::GRPCPORT_ATTR).AsString().data());
    }

    void UpdateRemoteLeaderInfo(const THashMap<TString, NYT::TNode>& attributes) {
        LeaderId = attributes.at(NCommonAttrs::ACTOR_NODEID_ATTR).AsUint64();
        LeaderResolver->SetLeaderHostPort(
            attributes.at(NCommonAttrs::HOSTNAME_ATTR).AsString() + ":" +
            attributes.at(NCommonAttrs::GRPCPORT_ATTR).AsString());
        if (attributes.contains(NCommonAttrs::REVISION_ATTR)) {
            LeaderRevision = attributes.at(NCommonAttrs::REVISION_ATTR).AsString();
        }
        UpdateLeaderInfo(attributes);
        YQL_CLOG(INFO, ProviderDq) << "Remote leader update: "
            << LeaderHost << ":" << LeaderPort << " nodeId=" << LeaderId;
    }

    void Bootstrap(const TActorContext& ctx) {
        Y_UNUSED(ctx);
        const auto& cfg = Coordinator->GetConfig();
        YQL_CLOG(INFO, ProviderDq) << "GWM Bootstrap: acquiring lock"
            << " lock_type=" << cfg.GetLockType()
            << " cluster=" << cfg.GetClusterName()
            << " prefix=" << cfg.GetPrefix()
            << " lock_path=" << cfg.GetPrefix() << "/locks/gwm";
        LockId = RegisterChild(Coordinator->CreateLock("gwm", false));
        YQL_CLOG(INFO, ProviderDq) << "GWM Bootstrap: lock actor registered, LockId=" << LockId;
    }

    TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parentId) override {
        return new IEventHandle(self, parentId, new TEvents::TEvBootstrap(), 0);
    }

    void OnRegisterNodeStub(TEvRegisterNode::TPtr& ev, const NActors::TActorContext&) {
        const auto& request = ev->Get()->Record.GetRequest();
        YQL_CLOG(TRACE, ProviderDq) << "OnRegisterNodeStub (GWM Initialization): immediate empty response"
            << " sender=" << ev->Sender
            << " nodeId=" << request.GetNodeId()
            << " role=" << request.GetRole();
        Send(ev->Sender, new TEvRegisterNodeResponse());
    }

    void OnRegisterNode(TEvRegisterNode::TPtr& ev, const NActors::TActorContext& ctx) {
        auto* mutableRequest = ev->Get()->Record.MutableRequest();
        auto& request = ev->Get()->Record.GetRequest();
        auto nodeId = request.GetNodeId();
        auto workerId = NYql::NDqs::NExecutionHelpers::GuidFromProto(request.GetGuid());
        auto role = request.GetRole();
        YQL_CLOG(TRACE, ProviderDq) << "OnRegisterNode (GWM Leader)"
            << " sender=" << ev->Sender
            << " nodeId=" << nodeId
            << " role=" << role
            << " workerId=" << GetGuidAsString(workerId)
            << " requestRevision=" << request.GetRevision()
            << " leaderRevision=" << Revision
            << " leaderEpoch=" << LeaderEpoch;
        Y_SCOPE_EXIT(&) {
            YQL_CLOG(TRACE, ProviderDq) << "OnRegisterNode: forwarding to nameservice"
                << " sender=" << ev->Sender
                << " nodeId=" << nodeId
                << " role=" << role;
            ctx.Send(ev->Forward(NActors::GetNameserviceActorId()));
        };

        if (role == "worker_node" && Revision == request.GetRevision()) {
            mutableRequest->SetEpoch(LeaderEpoch);
        }

        if (role == "service_node") {
            AllCriticalFiles.Add(
                workerId,
                nodeId,
                request.GetRevision(),
                request.GetAddress(),
                request.GetPid(),
                request.GetStartTime(),
                request.GetFilesOnNode());
        }

        if (role != "worker_node") {
            YQL_CLOG(DEBUG, ProviderDq) << "OnRegisterNode, mutableRequest->SetEpoch(LeaderEpoch), role " << role;
            mutableRequest->SetEpoch(LeaderEpoch);
            return;
        }

        int capacity = request.GetCapacity();
        if (capacity == 0) {
            capacity = 1;
        }

        {
            TWorkerInfo::TPtr workerInfo;
            bool needResume;

            auto newWorkerId = workerId;

            std::tie(workerInfo, needResume) = Workers.CreateOrUpdate(nodeId, newWorkerId, *mutableRequest);

            const auto now = TActivationContext::Now();
            for (auto i = StopFilters.begin(); i != StopFilters.end(); ) {
                if (i->IsExpired(now)) {
                    i = StopFilters.erase(i);
                    continue;
                }

                if (workerInfo && !workerInfo->IsDead && i->Match(*workerInfo, now)) {
                    Workers.SetStopping(workerInfo);
                    if (workerInfo->RunningRequests == 0 && workerInfo->RunningWorkerActors == 0) {
                        YQL_CLOG(DEBUG, ProviderDq) << "Stop worker on user request " << GetGuidAsString(workerId);
                        Send(MakeWorkerManagerActorID(nodeId), new TEvents::TEvPoison);
                        return;
                    }
                }

                ++i;
            }

            if (needResume) {
                MarkDirty();
            }

            if (workerInfo && !workerInfo->IsDead) {
                const auto uploadOptionsIt = FindIf(ResourceUploaderOptions, [&] (const auto& options) {
                    return options.YtBackend.GetClusterName() == workerInfo->ClusterName;
                });
                for (const auto& file : AllCriticalFiles.GetResources()) {
                    TWorkerInfo::TDownloadResource resource{
                        .File = file,
                    };
                    if (uploadOptionsIt != ResourceUploaderOptions.end()) {
                        // Warmup uploads use the GWM's configured backend for this worker's cluster.
                        resource.UploadPrefix = uploadOptionsIt->UploadPrefix;
                        resource.UploadClusterAddress = uploadOptionsIt->YtBackend.GetProxyAddress();
                    }
                    workerInfo->AddToDownloadList(resource);
                }

                THashMap<std::pair<TString, TString>, Yql::DqsProto::TDownloadGroup*> downloadGroups;
                mutableRequest->ClearDownloadGroups();
                mutableRequest->ClearDownloadList();
                for (const auto& [_, resource] : workerInfo->GetResourcesForDownloading()) {
                    auto& group = downloadGroups[std::make_pair(resource.UploadClusterAddress, resource.UploadPrefix)];
                    if (!group) {
                        group = mutableRequest->AddDownloadGroups();
                        group->SetUploadClusterAddress(resource.UploadClusterAddress);
                        group->SetUploadPrefix(resource.UploadPrefix);
                    }
                    *group->AddFiles() = resource.File;
                    // Old workers read DownloadList and use their configured upload location.
                    *mutableRequest->AddDownloadList() = resource.File;
                }
            }
        }
    }

    void CleanUp(TInstant now) {
        Workers.CleanUp(now, TDuration::Seconds(15));
    }

    void OnAllocateWorkersRequestStub(TEvAllocateWorkersRequest::TPtr& ev, const NActors::TActorContext&) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(ev->Get()->Record.GetTraceId());
        YQL_CLOG(WARN, ProviderDq) << "GWM still in Initialization (lock not yet acquired), rejecting allocation"
            << " LockId=" << LockId;
        Send(ev->Sender, new TEvAllocateWorkersResponse("GWM: Waiting for initialization", NYql::NDqProto::StatusIds::UNAVAILABLE));
    }

    void MarkDirty(size_t count = 0U) {
        ScheduleWaitCount = Min(ScheduleWaitCount, count);
    }

    void TryResume() {
        if (!Workers.Capacity() && ScheduleWaitCount > 0U) {
            Scheduler->ProcessAll([&] (const auto& item) {
                const TString error = "All DQ workers became unavailable while the operation was waiting";
                YQL_CLOG(DEBUG, ProviderDq) << "TGlobalWorkerManager::TryResume, ProcessAll: " << error;
                *OverloadedTotal += 1;
                *RejectedAfterWorkersLost += 1;
                Send(item.Sender, new TEvAllocateWorkersResponse(error, NYql::NDqProto::StatusIds::OVERLOADED));
                return true;
            });

            ScheduleWaitCount = 0U;
        } else if (Workers.FreeSlots() >= ScheduleWaitCount) {
            TWorkersStorage::TLargeSlotsBudgetByCluster remainingLargeSlotsByCluster;
            TWorkersStorage::TLargeSlotsBudgetByCluster* largeSlotsBudgetByCluster = nullptr;
            const auto process = [&] (const auto& item) {
                auto* clusterBudget = largeSlotsBudgetByCluster && item.Request.GetCount() > 1
                    ? largeSlotsBudgetByCluster
                    : nullptr;
                auto candidates = Workers.TryAllocate(item, clusterBudget);
                if (!candidates.empty()) {
                    DoAllocate(item, candidates);
                }
                return !candidates.empty();
            };

            if (LiteralHardReserve.IsEnabled()) {
                const auto state = GetLiteralHardReserveState();
                remainingLargeSlotsByCluster = state.LargeSlotsBudgetByCluster;
                largeSlotsBudgetByCluster = &remainingLargeSlotsByCluster;
                Scheduler->Process(
                    state.ProcessLimits,
                    process);
            } else {
                Scheduler->Process(
                    Workers.Capacity(),
                    Workers.FreeSlots(),
                    process);
            }
            ScheduleWaitCount = std::numeric_limits<size_t>::max();
        }
    }

    void OnAllocateWorkersRequest(TEvAllocateWorkersRequest::TPtr& ev, const NActors::TActorContext& ctx) {
        Y_UNUSED(ctx);
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(ev->Get()->Record.GetTraceId());
        YQL_CLOG(DEBUG, ProviderDq) << "TGlobalWorkerManager::TEvAllocateWorkersRequest"
            << " isForwarded=" << ev->Get()->Record.GetIsForwarded()
            << " files=" << ev->Get()->Record.GetFiles().size();
        for (const auto& file : ev->Get()->Record.GetFiles()) {
            YQL_CLOG(DEBUG, ProviderDq) << "AllocateRequest file: " << FormatFile(file);
        }

        TFailureInjector::Reach("allocate_workers_failure", [] { ::_exit(1); });

        const auto count = ev->Get()->Record.GetCount();
        if (count == 0) {
            Send(ev->Sender, new TEvAllocateWorkersResponse(
                "DQ allocation request count must be positive",
                NYql::NDqProto::StatusIds::BAD_REQUEST));
            return;
        }

        const auto runningTasksPerUserLimit = Scheduler->GetRunningTasksPerUserLimit();
        if (runningTasksPerUserLimit > 0 && count > runningTasksPerUserLimit) {
            YQL_CLOG(DEBUG, ProviderDq) << "TGlobalWorkerManager::TEvAllocateWorkersRequest: Request exceeds per-user running limit"
                << " (User: " << ev->Get()->Record.GetUser()
                << ", Count: " << count
                << ", Limit: " << runningTasksPerUserLimit << ")";
            Send(ev->Sender, new TEvAllocateWorkersResponse(
                TStringBuilder() << "Request needs " << count
                    << " DQ slots, exceeding the per-user running limit of " << runningTasksPerUserLimit,
                NYql::NDqProto::StatusIds::LIMIT_EXCEEDED));
            return;
        }

        if (!Workers.Capacity()) {
            const TString error = "No DQ workers are available to run the operation";
            YQL_CLOG(DEBUG, ProviderDq) << "TGlobalWorkerManager::TEvAllocateWorkersRequest: " << error;
            *OverloadedTotal += 1;
            *RejectedNoWorkers += 1;
            Send(ev->Sender, new TEvAllocateWorkersResponse(error, NYql::NDqProto::StatusIds::OVERLOADED));
            return;
        }

        const auto suspendResult = Scheduler->Suspend(NDq::IScheduler::TWaitInfo(ev->Get()->Record, ev->Sender));
        if (suspendResult.Status != NDq::IScheduler::ESuspendStatus::Accepted) {
            const bool perUserLimit = suspendResult.Status == NDq::IScheduler::ESuspendStatus::PerUserLimit;
            const TString error = TStringBuilder()
                << "Too many dq operations: "
                << (perUserLimit
                    ? "per-user queue limit reached"
                    : "global large-operation queue limit reached")
                << " (waiting: " << suspendResult.WaitingOperations
                << ", limit: " << suspendResult.Limit << ")";
            YQL_CLOG(DEBUG, ProviderDq) << "TGlobalWorkerManager::TEvAllocateWorkersRequest: " << error;
            *OverloadedTotal += 1;
            Send(ev->Sender, new TEvAllocateWorkersResponse(error, NYql::NDqProto::StatusIds::OVERLOADED));
            return;
        }

        auto [_, error]  = MaybeUpload(ev->Get()->Record.GetIsForwarded(), TVector<TFileResource>(ev->Get()->Record.GetFiles().begin(),ev->Get()->Record.GetFiles().end()));
        if (!error.empty()) {
            YQL_CLOG(WARN, ProviderDq) << "TGlobalWorkerManager::TEvAllocateWorkersRequest: MaybeUpload error: " << error;
            Scheduler->ProcessAll([&] (const auto& item) {
                return item.Sender == ev->Sender &&
                    item.Request.GetTraceId() == ev->Get()->Record.GetTraceId();
            });
            Send(ev->Sender, new TEvAllocateWorkersResponse(error, NYql::NDqProto::StatusIds::EXTERNAL_ERROR));
            return;
        }

        YQL_CLOG(DEBUG, ProviderDq) << "TGlobalWorkerManager::TEvAllocateWorkersRequest: MarkDirty " << count;
        MarkDirty(count);
    }

    void DecrLiteralQueries(const TString& clusterName) {
        *LiteralQueries[clusterName] += -1;
    }

    void IncrLiteralQueries(const TString& clusterName) {
        auto& counter = LiteralQueries[clusterName];
        if (!counter) {
            counter = Metrics
                ->GetSubgroup("counters", "gwm")
                ->GetSubgroup("component", "lists")
                ->GetSubgroup("ClusterName", clusterName)
                ->GetCounter("LiteralQueries");
        }
        *counter += 1;
    }

    void DoAllocate(const NDq::IScheduler::TWaitInfo& waitInfo, const TVector<TWorkerInfo::TPtr>& allocated) {
        YQL_CLOG(DEBUG, ProviderDq) << "TGlobalWorkerManager::DoAllocate";
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(waitInfo.Request.GetTraceId());
        Y_ABORT_UNLESS(allocated.size() == waitInfo.Request.GetCount());

        THashSet<TString> clusters;
        const bool trackLargeSlots = allocated.size() > 1 && LiteralHardReserve.IsEnabled();

        for (const auto& workerInfo : allocated) {
            YQL_CLOG(DEBUG, ProviderDq) << "Allocating worker " << GetGuidAsString(workerInfo->WorkerId);
            YQL_CLOG(DEBUG, ProviderDq) << " Address : " << workerInfo->Address;
            for (const auto& [k, v] : workerInfo->Attributes) {
                YQL_CLOG(DEBUG, ProviderDq) << " " << k << " : " << v;
            }
            clusters.insert(workerInfo->ClusterName);
            if (trackLargeSlots && LiteralHardReserve.Find(workerInfo->ClusterName)) {
                // Each entry is a task slot; one worker may occur several times.
                ++CurrentEpochLargeSlotsByCluster[workerInfo->ClusterName];
            }
        }

        THashSet<TString> usedFiles;
        for (const auto& file : waitInfo.Request.GetFiles()) {
            usedFiles.insert(file.GetObjectId());
        }

        auto resourceId = CurrentResourceId.Data;
        CurrentResourceId.Counter ++;

        auto resourceAllocator = waitInfo.Sender;

        AllocatedResources[resourceId] = {
            .ActorId = resourceAllocator,
            .User = waitInfo.Request.GetUser(),
            .Workers = allocated,
            .UsedFiles = usedFiles,
            .Clusters = clusters,
            .StartTime = TInstant::Now(),
        };

        auto response = MakeHolder<TEvAllocateWorkersResponse>(resourceId, allocated);
        waitInfo.Stat.FlushCounters(response->Record);
        Send(
            resourceAllocator,
            response.Release(),
            IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);

        Subscribe(resourceAllocator.NodeId());

        if (allocated.size() == 1) {
            Y_ABORT_UNLESS(clusters.size() == 1);
            IncrLiteralQueries(*clusters.begin());
        }
    }

    void DropActorOrNode(
        const std::function<bool(TActorId actorId)>& check,
        const THashSet<TGUID>& failedWorkers = {})
    {
        YQL_CLOG(DEBUG, ProviderDq) << "DropActorOrNode";
        TVector<ui64> freeList;
        for (const auto& [k, v] : AllocatedResources) {
            if (check(v.ActorId)) {
                freeList.push_back(k);
            }
        }

        for (auto resourceId : freeList) {
            FreeResource(resourceId, failedWorkers);
        }

        Scheduler->ProcessAll([&] (const auto& item) {
            return check(item.Sender);
        });

        MarkDirty();
    }

    void OnUndelivered(TEvents::TEvUndelivered::TPtr& ev, const NActors::TActorContext& /*ctx*/)
    {
        auto deadActor = ev->Sender;
        YQL_CLOG(DEBUG, ProviderDq) << "OnUndelivered"
            << " sender=" << deadActor
            << " reason=" << ev->Get()->Reason;
        DropActorOrNode([&](TActorId actorId) {
            return actorId == deadActor;
        });
    }

    void OnExecuterConnectedStub(TEvInterconnect::TEvNodeConnected::TPtr& ev, const NActors::TActorContext&) {
        YQL_CLOG(DEBUG, ProviderDq) << "TGlobalWorkerManager::OnExecuterConnectedStub " << ev->Get()->NodeId;
    }

    void OnExecuterDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev, const NActors::TActorContext& /*ctx*/)
    {
        YQL_CLOG(DEBUG, ProviderDq) << "OnExecuterDisconnected " << ev->Get()->NodeId;

        Unsubscribe(ev->Get()->NodeId);

        auto deadNode = ev->Get()->NodeId;

        DropActorOrNode([&](TActorId actorId) {
            return actorId.NodeId() == deadNode;
        });
    }

    void OnFreeWorkersStub(TEvFreeWorkersNotify::TPtr& ev, const NActors::TActorContext&) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(ev->Get()->Record.GetTraceId());
        YQL_CLOG(DEBUG, ProviderDq) << "TGlobalWorkerManager::OnFreeWorkersStub " << ev->Sender.NodeId();
    }

    void FreeResource(ui64 resourceId, const THashSet<TGUID>& failedWorkers) {
        YQL_CLOG(DEBUG, ProviderDq) << "TGlobalWorkerManager::FreeResource " << resourceId;
        const auto resourceIt = AllocatedResources.find(resourceId);
        if (resourceIt == AllocatedResources.end()) {
            return;
        }

        auto& resource = resourceIt->second;

        bool rebuildLargeSlots = false;
        if (LiteralHardReserve.IsEnabled() && resource.Workers.size() > 1) {
            for (const auto& worker : resource.Workers) {
                if (!LiteralHardReserve.Find(worker->ClusterName)) {
                    continue;
                }
                auto activeIt = CurrentEpochLargeSlotsByCluster.find(worker->ClusterName);
                if (activeIt == CurrentEpochLargeSlotsByCluster.end() || activeIt->second == 0) {
                    YQL_CLOG(ERROR, ProviderDq) << "Inconsistent literal hard reserve accounting"
                        << " resourceId=" << resourceId << " cluster=" << worker->ClusterName;
                    rebuildLargeSlots = true;
                    break;
                }
                if (--activeIt->second == 0) {
                    CurrentEpochLargeSlotsByCluster.erase(activeIt);
                }
            }
        }

        auto now = TInstant::Now();

        for (const auto& worker : resource.Workers) {
            YQL_CLOG(DEBUG, ProviderDq) << "Free worker " << GetGuidAsString(worker->WorkerId);

            if (failedWorkers.contains(worker->WorkerId)) {
                Workers.DropWorker(now, worker->WorkerId);
            } else {
                Workers.FreeWorker(now, worker);
            }
        }

        if (resource.Workers.size() == 1) {
            DecrLiteralQueries(*resource.Clusters.begin());
        }

        Workers.UpdateResourceUseTime(now - resource.StartTime, resource.UsedFiles);
        Scheduler->ReleaseRunningTasks(resource.User, resource.Workers.size());
        AllocatedResources.erase(resourceIt);

        if (rebuildLargeSlots) {
            // Do not leave an understated count that would allow consuming the reserve.
            CurrentEpochLargeSlotsByCluster.clear();
            for (const auto& [_, remainingResource] : AllocatedResources) {
                if (remainingResource.Workers.size() <= 1) {
                    continue;
                }
                for (const auto& worker : remainingResource.Workers) {
                    if (LiteralHardReserve.Find(worker->ClusterName)) {
                        ++CurrentEpochLargeSlotsByCluster[worker->ClusterName];
                    }
                }
            }
        }
    }

    void OnFreeWorkers(TEvFreeWorkersNotify::TPtr& ev, const TActorContext& /*ctx*/) {
        auto resourceId = ev->Get()->Record.GetResourceId();
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(ev->Get()->Record.GetTraceId());
        YQL_CLOG(DEBUG, ProviderDq) << "TEvFreeWorkersNotify " << resourceId;

        THashSet<TGUID> failedWorkers;
        for (const auto& workerGuid : ev->Get()->Record.GetFailedWorkerGuid()) {
            YQL_CLOG(DEBUG, ProviderDq) << "Failed worker: " << workerGuid;
            failedWorkers.insert(GetGuid(workerGuid));
        }

        if (resourceId == 0) {
            // Zero id means the allocator died before it learned its resource id.
            DropActorOrNode(
                [&] (TActorId actorId) {
                    return actorId == ev->Sender;
                },
                failedWorkers);
            return;
        }

        if (AllocatedResources.contains(resourceId)) {
            FreeResource(resourceId, failedWorkers);
        }

        MarkDirty();
    }

    void OnJobStop(TEvJobStop::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);

        auto request = ev->Get()->Record.GetRequest();
        TString requestStr;
        TStringOutput output1(requestStr);
        SerializeToTextFormat(request, output1);

        YQL_CLOG(DEBUG, ProviderDq) << "JobStop " << requestStr;

        const auto now = TActivationContext::Now();
        StopFilters.remove_if([now](const TWorkerStopFilter& filter) {
            return filter.IsExpired(now);
        });

        TWorkerStopFilter filter(request, now);
        if (!filter.IsValid()) {
            const TString error = TStringBuilder()
                << "Invalid JobStop TTL: " << request.GetTtlSeconds() << " seconds";
            YQL_CLOG(ERROR, ProviderDq) << error;
            Send(ev->Sender, new TEvJobStopResponse(error));
            return;
        }

        Workers.Visit([&](const TWorkerInfo::TPtr& workerInfo) {
            if (!workerInfo->IsDead && filter.Match(*workerInfo, now)) {
                Workers.SetStopping(workerInfo);
                if (request.GetForce() ||
                    (workerInfo->RunningRequests == 0 && workerInfo->RunningWorkerActors == 0))
                {
                    YQL_CLOG(DEBUG, ProviderDq) << "Stop worker on user request " << GetGuidAsString(workerInfo->WorkerId);
                    Send(MakeWorkerManagerActorID(workerInfo->NodeId), new TEvents::TEvPoison);
                }
            }
        });

        if (!request.GetForce() && !filter.IsExpired(now)) {
            StopFilters.push_back(std::move(filter));
        }

        Send(ev->Sender, new TEvJobStopResponse());
    }

    void OnJobStopStub(TEvJobStop::TPtr& ev, const TActorContext&) {
        Send(ev->Sender, new TEvJobStopResponse("GWM: Waiting for initialization", true));
    }

    void OnClusterStatusStub(TEvClusterStatus::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        auto response = MakeHolder<TEvClusterStatusResponse>();
        Send(ev->Sender, response.Release());
    }

    void OnClusterStatus(TEvClusterStatus::TPtr& ev, const TActorContext& ctx) {
        YQL_CLOG(DEBUG, ProviderDq) << "TEvClusterStatus ";

        Y_UNUSED(ctx);

        auto response = MakeHolder<TEvClusterStatusResponse>();

        auto* r = response->Record.MutableResponse();

        r->SetRevision(Revision);

        Workers.ClusterStatus(r);

        Scheduler->ForEach([&] (const auto& item) {
            auto* info = r->AddWaitList();
            info->SetCount(item.Request.GetCount());
            info->SetOperationId(item.Request.GetTraceId());
            info->SetUserName(item.Request.GetUser());
            info->SetEnqueueTime(item.StartTime.ToString());

            THashMap<TString, Yql::DqsProto::ClusterStatusResponse::File> files;

            for (const auto& rec : item.Request.GetFiles()) {
                auto& file = files[rec.GetObjectId()];
                file.SetObjectId(rec.GetObjectId());
                file.SetName(rec.GetName());
                if (item.ResLeft.contains(rec.GetObjectId())) {
                    file.SetLeft(item.ResLeft[rec.GetObjectId()]);
                }
                file.SetSize(rec.GetSize());
            }

            for (const auto& it : item.Request.GetWorkerFilterPerTask()) {
                for (const auto& rec : it.GetFile()) {
                    auto& file = files[rec.GetObjectId()];
                    file.SetObjectId(rec.GetObjectId());
                    file.SetName(rec.GetName());
                    file.SetCount(file.GetCount()+1);
                    file.SetSize(rec.GetSize());
                }
            }

            for (const auto& [_, v] : files) {
                *info->AddResources() = v;
            }
        });

        for (const auto& i : Uploading) {
            *r->AddUploading() = i.first;
        }

        for (const auto& [guid, node] : AllCriticalFiles.GetNodes()) {
            auto* nodeInfo = r->AddServiceNode();
            nodeInfo->SetRevision(node.Revision);
            nodeInfo->SetNodeId(node.NodeId);
            nodeInfo->SetPid(node.Pid);
            nodeInfo->SetAddress(node.Address);
            nodeInfo->SetStartTime(node.StartTime);
            nodeInfo->SetGuid(GetGuidAsString(guid));
            for (const auto& file : node.OrderedFiles) {
                *nodeInfo->AddFile() = file;
            }
        }

        Send(ev->Sender, response.Release());
    }

    void OnQueryStatusStub(TEvQueryStatus::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        auto response = MakeHolder<TEvQueryStatusResponse>();
        Send(ev->Sender, response.Release());
    }

    void OnQueryStatus(TEvQueryStatus::TPtr& ev, const TActorContext& ctx) {
        // TODO: remove me YQL-18071. Executer actor is responsible for this function
        Y_UNUSED(ctx);

        auto sessionId = ev->Get()->Record.request().GetSession();

        bool queued = false;
        ui32 workersCount;

        Scheduler->ForEach([&queued, &sessionId, &workersCount] (const NDq::IScheduler::TWaitInfo& item) {
            if (sessionId == item.Request.GetTraceId()) {
                queued = true;
                workersCount = item.Request.GetCount();
            }
        });

        auto response = MakeHolder<TEvQueryStatusResponse>();

        auto* r = response->Record.MutableResponse();

        if (queued) {
            if (workersCount > Workers.FreeSlots()) {
                r->SetStatus("Awaiting");
            } else {
                r->SetStatus("Uploading artifacts");
            }
        } else {
            r->SetStatus("Executing");
        }

        Send(ev->Sender, response.Release());
    }

    void OnIsReadyStub(TEvIsReady::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);

        auto response = MakeHolder<TEvIsReadyResponse>();
        Send(ev->Sender, response.Release());
    }

    void OnOperationStopStub(TEvOperationStop::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);

        auto response = MakeHolder<TEvOperationStopResponse>();
        Send(ev->Sender, response.Release());
    }

    void OnOperationStop(TEvOperationStop::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        YQL_CLOG(DEBUG, ProviderDq) << "OnOperationStop";

        Scheduler->ProcessAll([&] (const auto& item) {
            if (item.Request.GetTraceId() == ev->Get()->Record.GetRequest().GetOperationId()) {
                Send(item.Sender, new TEvDqFailure(NYql::NDqProto::StatusIds::ABORTED, TIssue("Operation stopped by scheduler").SetCode(TIssuesIds::DQ_GATEWAY_ERROR, TSeverityIds::S_ERROR)));
                return true;
            }
            return false;
        });

        auto response = MakeHolder<TEvOperationStopResponse>();
        Send(ev->Sender, response.Release());
    }

    void FillCriticalFiles(TEvIsReady::TPtr& ev) {
        if (TerminatingMode) {
            return;
        }
        if (ev->Get()->Record.GetIsForwarded()) {
            return;
        }
        LocalCriticalFiles->Clear();
        for (auto& f : ev->Get()->Record.GetRequest().GetFiles()) {
            //TODO: Add support types in LocalCriticalFiles
            //Don't uncomment this
            //if (f.GetObjectType() == TWorkerInfo::TFileResource::EEXE_FILE) {
                // LocalCriticalFiles supports only udfs
            //    continue;
            //}
            LocalCriticalFiles->AddFile("unused", f.GetObjectId());
        }
        UpdateLocalCriticalFiles();
    }

    void UpdateLocalCriticalFiles() {
        TVector<TFileResource> files;
        LocalCriticalFiles->Walk([&](const TString& objectId) {
            TFileResource r;
            r.SetObjectId(objectId);
            files.push_back(r);
        });
        TFileResource r;
        r.SetObjectId(Revision);
        files.push_back(r);

        AllCriticalFiles.Add(TGUID(), SelfId().NodeId(), Revision, Coordinator->GetIp(), Pid, TString(), files);
    }

    void OnIsReadyForward(TEvIsReady::TPtr& ev, const TActorContext& ctx) {
        auto [_, error]  = MaybeUpload(ev->Get()->Record.GetIsForwarded(), TVector<TFileResource>(ev->Get()->Record.GetRequest().GetFiles().begin(),ev->Get()->Record.GetRequest().GetFiles().end()), true);
        if (!error.empty()) {
            YQL_CLOG(WARN, ProviderDq) << "TEvIsReady error on upload: " << error;
        }

        if (TerminatingMode) {
            auto response = MakeHolder<TEvIsReadyResponse>();
            response->Record.SetIsReady(true);
            Send(ev->Sender, response.Release());
        } else {
            FillCriticalFiles(ev);
            ev->Get()->Record.SetIsForwarded(true);
            ctx.Send(ev->Forward(GetGlobalWorkerManagerActorId(LeaderId)));
        }
    }

    void OnIsReady(TEvIsReady::TPtr& ev, const TActorContext& ctx) {
        YQL_CLOG(DEBUG, ProviderDq) << "TEvIsReady ";
        Y_UNUSED(ctx);

        FillCriticalFiles(ev);

        auto [_, error]  = MaybeUpload(ev->Get()->Record.GetIsForwarded(), TVector<TFileResource>(ev->Get()->Record.GetRequest().GetFiles().begin(),ev->Get()->Record.GetRequest().GetFiles().end()), true);
        if (!error.empty()) {
            YQL_CLOG(WARN, ProviderDq) << "TEvIsReady error on upload: " << error;
        }
        THashMap<TString , std::pair<ui32, ui32>> clusterMap;
        for (const auto& options : ResourceUploaderOptions) {
            clusterMap.emplace(options.YtBackend.GetClusterName(), std::make_pair(options.YtBackend.GetMaxJobs(), 0));
        }

        auto resources = ev->Get()->Record.GetRequest().GetFiles();
        Workers.IsReady(TVector<TFileResource>(resources.begin(), resources.end()), clusterMap);

        // any cluster has at least 50% of workers with actual vanilla job ready
        bool isReady = false;
        bool hasCommunalBackends = false;
        ui32 totalWorkers = Workers.GetList().size();
        for (const auto& [cluster,pair] : clusterMap) {
            YQL_CLOG(DEBUG, ProviderDq) << cluster << " ready: " << pair.second << "/" << pair.first
                << " workers ready (total_workers=" << totalWorkers << ", required_files=" << resources.size() << ")";
            if (pair.first > 0) {
                hasCommunalBackends = true;
                isReady |= pair.second * 2 >= pair.first;
            }
        }
        isReady = !hasCommunalBackends || isReady;

        YQL_CLOG(DEBUG, ProviderDq) << "IsReady status : " << isReady;

        auto response = MakeHolder<TEvIsReadyResponse>();
        response->Record.SetIsReady(isReady);

        Send(ev->Sender, response.Release());
    }

    void OnGetMasterStub(TEvGetMasterRequest::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        auto response = MakeHolder<TEvGetMasterResponse>();
        Send(ev->Sender, response.Release());
    }

    void OnGetMaster(TEvGetMasterRequest::TPtr& ev, const TActorContext& ctx) {
        YQL_CLOG(DEBUG, ProviderDq) << "TEvGetMasterRequest ";

        Y_UNUSED(ctx);

        auto response = MakeHolder<TEvGetMasterResponse>();

        auto* r = response->Record.MutableResponse();

        r->SetHost(LeaderHost);
        r->SetPort(LeaderPort);

        Send(ev->Sender, response.Release());
    }

    void OnConfigureFailureInjectorStub(TEvConfigureFailureInjectorRequest::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        auto response = MakeHolder<TEvConfigureFailureInjectorResponse>();
        Send(ev->Sender, response.Release());
    }

    void OnConfigureFailureInjector(TEvConfigureFailureInjectorRequest::TPtr& ev, const TActorContext& ctx) {
        YQL_CLOG(DEBUG, ProviderDq) << "TEvConfigureFailureInjectorRequest ";

        Y_UNUSED(ctx);

        // TODO: support resending the event to all workers and gathering the results?
        auto& request = ev->Get()->Record.GetRequest();
        // just forward the event to a worker node
        if (request.GetNodeId() == SelfId().NodeId()) {
            TFailureInjector::Set(request.GetName(), request.GetSkip(), request.GetCountOfFails());

            auto response = MakeHolder<TEvConfigureFailureInjectorResponse>();
            auto* r = response->Record.MutableResponse();
            r->Setsuccess(true);
            Send(ev->Sender, response.Release());
        } else {
            ctx.Send(ev->Forward(MakeWorkerManagerActorID(request.GetNodeId())));
        }
    }

    TLiteralHardReserveState GetLiteralHardReserveState() const {
        TLiteralHardReserveState state;
        // Keep the scheduler's existing global limits independent of hard-reserve settings.
        // The schedulable snapshot is used only to derive the new large-request budget.
        state.ProcessLimits = {
            .TotalSlots = Workers.Capacity(),
            .FreeSlots = Workers.FreeSlots(),
            .LargeSlotsBudget = Workers.FreeSlots(),
        };

        if (!LiteralHardReserve.IsEnabled()) {
            return state;
        }

        const auto& snapshot = Workers.GetSlotsSnapshot();
        state.ProcessLimits.LargeSlotsBudget = snapshot.Free;

        for (const auto& [clusterName, settings] : LiteralHardReserve.GetClusters()) {
            auto& cluster = state.Clusters[clusterName];
            cluster.ConfiguredPercent = settings.Percent;
            cluster.ConfiguredMinSlots = settings.MinSlots;
            ui64 capacity = 0;
            ui64 freeSlots = 0;
            if (const auto snapshotIt = snapshot.Clusters.find(clusterName); snapshotIt != snapshot.Clusters.end()) {
                capacity = snapshotIt->second.Capacity;
                freeSlots = snapshotIt->second.Free;
            }
            ui64 currentEpochLargeSlots = 0;
            if (const auto activeIt = CurrentEpochLargeSlotsByCluster.find(clusterName); activeIt != CurrentEpochLargeSlotsByCluster.end()) {
                currentEpochLargeSlots = activeIt->second;
            }

            cluster.Values = TLiteralHardReserveValues::Calculate(
                capacity,
                freeSlots,
                settings.CalculateReserve(capacity),
                currentEpochLargeSlots);
            const auto& values = cluster.Values;

            state.LargeSlotsBudgetByCluster[clusterName] = values.LargeBudget;
            state.ProcessLimits.LargeSlotsBudget -= values.FreeSlots;
            state.ProcessLimits.LargeSlotsBudget += values.LargeBudget;
        }
        return state;
    }

    void UpdateMetrics(bool updateRunningLimitedQueueSize) {
        if (!WaitListSize) {
            WaitListSize = Metrics->GetSubgroup("component", "lists")->GetCounter("WaitListSize");
        }
        *WaitListSize = Scheduler->UpdateMetrics(updateRunningLimitedQueueSize);
        HardReserveCounters.Update(GetLiteralHardReserveState());
        Workers.UpdateMetrics();
    }

private:
    TActorId GetGlobalWorkerManagerActorId(ui32 nodeId) const {
        return ActorIdOptions.UseGlobalActorId
            ? MakeGlobalWorkerManagerActorID(nodeId)
            : MakeWorkerManagerActorID(nodeId);
    }

    const ICoordinationHelper::TPtr Coordinator;
    const std::shared_ptr<TSingleNodeResolver> LeaderResolver;
    TActorId LeaderPinger;
    TActorId LockId;

    TSensorsGroupPtr Metrics;
    TSensorsGroupPtr UploaderMetrics;
    THistogramPtr LatencyHistogram;
    const TDynamicCounters::TCounterPtr AllocateWorkersWithExeFileCounter;
    const TDynamicCounters::TCounterPtr AllocateWorkersWithoutExeFileCounter;
    const TDynamicCounters::TCounterPtr OverloadedTotal;
    const TDynamicCounters::TCounterPtr RejectedNoWorkers;
    const TDynamicCounters::TCounterPtr RejectedAfterWorkersLost;
    THashMap<TString,NMonitoring::TDynamicCounters::TCounterPtr> LiteralQueries;
    const TLiteralRequestsHardReserveSettings LiteralHardReserve;
    TWorkersStorage Workers;
    const TLiteralHardReserveCounters HardReserveCounters;

    struct TResourceInfo {
        TActorId ActorId;
        TString User;
        TVector<TWorkerInfo::TPtr> Workers;
        THashSet<TString> UsedFiles;
        THashSet<TString> Clusters;
        TInstant StartTime;
    };
    THashMap<ui64, TResourceInfo> AllocatedResources;
    THashMap<TString, ui64> CurrentEpochLargeSlotsByCluster;

    ui32 LeaderId = static_cast<ui32>(-1);
    TString LeaderRevision;
    TString LeaderHost;
    ui32 LeaderPort = 0u;

    ui32 LeaderEpoch = 0;
    TDqResourceId CurrentResourceId;

    const NDq::IScheduler::TPtr Scheduler;
    const TString Revision;

    THashMap<TActorId, TVector<TString>> UploadProcesses; // actorId -> objects

    struct TUploadingInfo {
        EFileType ObjectType;
        TString ObjectName;
        int Clusters;
        i64 Size;
    };
    THashMap<TString, TUploadingInfo> Uploading; // objectId -> objectName

    TVector<TResourceManagerOptions> ResourceUploaderOptions;
    const TGlobalWorkerManagerActorIdOptions ActorIdOptions;

    TList<TWorkerStopFilter> StopFilters;

    TDynamicCounters::TCounterPtr WaitListSize;

    // filled in IsReady
    IFileCache::TPtr LocalCriticalFiles = MakeIntrusive<TLocalCriticalFiles>();
    // filled in IsReady for local files
    // filled in OnRegister for other files
    TAllCriticalFiles AllCriticalFiles;

    THashMap<TString, TInstant> LastUploadCache;

    // Don't reschedule too frequently to avoid GWM hanging
    TDuration ScheduleInterval;
    size_t ScheduleWaitCount = std::numeric_limits<size_t>::max(); // max - no, 0 - any, > 0 count
    TInstant LastCleanTime;
    TDuration CleanInterval = TDuration::Seconds(2);
    bool TerminatingMode = false;
    const ui32 Pid = GetPID();
};

NActors::IActor* CreateGlobalWorkerManager(
        const ICoordinationHelper::TPtr& coordinator,
        const TVector<TResourceManagerOptions>& resourceUploaderOptions,
        IMetricsRegistryPtr metricsRegistry,
        const NProto::TDqConfig::TScheduler& schedulerConfig,
        TDuration scheduleInterval,
        TGlobalWorkerManagerActorIdOptions actorIdOptions) {
    return new TGlobalWorkerManager(
        coordinator,
        resourceUploaderOptions,
        std::move(metricsRegistry),
        schedulerConfig,
        scheduleInterval,
        actorIdOptions);
}

} // namespace NYql
