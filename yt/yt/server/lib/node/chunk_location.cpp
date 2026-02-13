#include "chunk_location.h"

#include "chunk.h"
#include "private.h"

#include <yt/yt/server/lib/io/dynamic_io_engine.h>
#include <yt/yt/server/lib/io/io_workload_model.h>

#include <yt/yt/server/lib/misc/disk_health_checker.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/misc/fair_share_hierarchical_queue.h>
#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/profiling/public.h>

namespace NYT::NNode {

// Others must not be able to list chunk store and chunk cache directories.
static constexpr int ChunkFilesPermissions = 0751;

////////////////////////////////////////////////////////////////////////////////

using namespace NProfiling;
using namespace NIO;
using namespace NServer;
using namespace NObjectClient;
using namespace NChunkClient;
using namespace NFS;

////////////////////////////////////////////////////////////////////////////////

TLocationFairShareSlot::TLocationFairShareSlot(
    TFairShareHierarchicalSlotQueuePtr<std::string> queue,
    TFairShareHierarchicalSlotQueueSlotPtr<std::string> slot)
    : Queue_(std::move(queue))
    , Slot_(std::move(slot))
{
    YT_VERIFY(Queue_);
    YT_VERIFY(Slot_);
}

TFairShareHierarchicalSlotQueueSlotPtr<std::string> TLocationFairShareSlot::GetSlot() const
{
    return Slot_;
}

TLocationFairShareSlot::~TLocationFairShareSlot()
{
    if (Slot_) {
        Queue_->DequeueSlot(Slot_);
        Slot_->ReleaseResources();
        Slot_.Reset();
    }
}

////////////////////////////////////////////////////////////////////////////////

TChunkLocationBase::TChunkLocationBase(
    ELocationType type,
    TString id,
    TChunkLocationConfigBasePtr config,
    TCallback<TBriefChunkLocationConfig()> getBriefConfigCallback,
    TCellId cellId,
    const TFairShareHierarchicalSchedulerPtr<std::string>& fairShareHierarchicalScheduler,
    const IHugePageManagerPtr& hugePageManager,
    const NLogging::TLogger& logger,
    const NProfiling::TProfiler& profiler)
    : TDiskLocation(
        StaticPointerCast<TDiskLocationConfig>(config),
        std::move(id),
        std::move(logger))
    , CellId_(cellId)
    , Type_(type)
    , StaticConfig_(std::move(config))
    , RuntimeConfig_(StaticConfig_)
    , GetBriefConfigCallback_(std::move(getBriefConfigCallback))
{
    TTagSet tagSet;
    tagSet.AddTag({"location_type", FormatEnum(Type_)});
    tagSet.AddTag({"disk_family", StaticConfig_->DiskFamily}, -1);
    // NB(pogorelov): Medium should be set in derived classes.
    tagSet.AddTag({"medium", "<unknown>"}, -1);
    tagSet.AddTag({"location_id", Id_}, -1);
    tagSet.AddExtensionTag({"device_name", StaticConfig_->DeviceName}, -1);
    tagSet.AddExtensionTag({"device_model", StaticConfig_->DeviceModel}, -1);

    MediumTag_ = tagSet.AddDynamicTag(2);

    Profiler_ = profiler
        .WithSparse()
        .WithTags(tagSet);

    IOFairShareQueue_ = CreateFairShareHierarchicalSlotQueue<std::string>(
        fairShareHierarchicalScheduler,
        Profiler_.WithPrefix("/fair_share_hierarchical_queue"));

    HugePageManager_ = hugePageManager;

    DynamicIOEngine_ = CreateDynamicIOEngine(
        StaticConfig_->IOEngineType,
        StaticConfig_->IOConfig,
        IOFairShareQueue_,
        HugePageManager_,
        Id_,
        Profiler_,
        Logger.WithTag("LocationId: %v", Id_));
    IOEngineModel_ = CreateIOModelInterceptor(
        Id_,
        DynamicIOEngine_,
        Logger.WithTag("IOModel: %v", Id_));
    IOEngine_ = IOEngineModel_;

    HealthChecker_ = New<TDiskHealthChecker>(
        StaticConfig_->DiskHealthChecker,
        GetPath(),
        GetAuxPoolInvoker(),
        Logger,
        Profiler_);
}

const NIO::IIOEnginePtr& TChunkLocationBase::GetIOEngine() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return IOEngine_;
}

i64 TChunkLocationBase::GetLegacyWriteMemoryLimit() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto config = GetRuntimeConfig();
    return config->LegacyWriteMemoryLimit;
}

i64 TChunkLocationBase::GetReadMemoryLimit() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto config = GetRuntimeConfig();
    return config->ReadMemoryLimit;
}

i64 TChunkLocationBase::GetWriteMemoryLimit() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto config = GetRuntimeConfig();
    return config->WriteMemoryLimit;
}

i64 TChunkLocationBase::GetTotalMemoryLimit() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto config = GetRuntimeConfig();
    return config->TotalMemoryLimit;
}

i64 TChunkLocationBase::GetSessionCountLimit() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto config = GetRuntimeConfig();
    return config->SessionCountLimit;
}

void TChunkLocationBase::Reconfigure(TChunkLocationConfigBasePtr config)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    TDiskLocation::Reconfigure(StaticPointerCast<TDiskLocationConfig>(config));

    HealthChecker_->Reconfigure(config->DiskHealthChecker);

    DynamicIOEngine_->SetType(config->IOEngineType, config->IOConfig);

    RuntimeConfig_.Store(std::move(config));
}

ELocationType TChunkLocationBase::GetType() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return Type_;
}

TChunkLocationUuid TChunkLocationBase::GetUuid() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return Uuid_;
}

TChunkLocationIndex TChunkLocationBase::GetIndex() const
{
    return Index_;
}

void TChunkLocationBase::SetIndex(TChunkLocationIndex index)
{
    if (Index_ != NNodeTrackerClient::InvalidChunkLocationIndex && Index_ != index) {
        YT_LOG_ALERT(
            "Attempted to change chunk location index (LocationUuid: %v, OldIndex: %v, NewIndex: %v)",
            Uuid_,
            Index_,
            index);

        THROW_ERROR_EXCEPTION("Attempted to change chunk location index")
            << TErrorAttribute("location_uuid", Uuid_)
            << TErrorAttribute("old_index", Index_)
            << TErrorAttribute("new_index", index);
    }
    Index_ = index;
}

const TString& TChunkLocationBase::GetDiskFamily() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return StaticConfig_->DiskFamily;
}

const NProfiling::TProfiler& TChunkLocationBase::GetProfiler() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return Profiler_;
}

const TString& TChunkLocationBase::GetPath() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return StaticConfig_->Path;
}

i64 TChunkLocationBase::GetQuota() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return StaticConfig_->Quota.value_or(std::numeric_limits<i64>::max());
}

const IInvokerPtr& TChunkLocationBase::GetAuxPoolInvoker()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return IOEngine_->GetAuxPoolInvoker();
}

std::vector<TChunkDescriptor> TChunkLocationBase::Scan()
{
    YT_ASSERT_INVOKER_AFFINITY(GetAuxPoolInvoker());
    YT_VERIFY(GetState() == ELocationState::Enabling);

    try {
        ValidateLockFile();
        ValidateMinimumSpace();
        ValidateWritable();
        return DoScan();
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Location disabled");
        MarkUninitializedLocationDisabled(ex);
        return {};
    }
}

void TChunkLocationBase::InitializeIds()
{
    try {
        InitializeCellId();
        InitializeUuid();
    } catch (const std::exception& ex) {
        Crash(TError("Location initialize failed") << ex);
    }
}

void TChunkLocationBase::Start()
{
    ChangeState(ELocationState::Enabled);
    LocationDisabledAlert_.Store(TError());

    try {
        DoStart();
    } catch (const std::exception& ex) {
        ScheduleDisable(TError("Location start failed") << ex);
    }
}

void TChunkLocationBase::SubscribeDiskCheckFailed(const TCallback<void(const TError&)> callback)
{
    HealthChecker_->SubscribeFailed(callback);
}

bool TChunkLocationBase::OnDiskRepaired()
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    if (!ChangeState(ELocationState::Disabled, ELocationState::Destroyed)) {
        return false;
    }

    LocationDiskFailedAlert_.Store(TError());
    return true;
}

void TChunkLocationBase::UpdateUsedSpace(i64 size)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    UsedSpace_ += size;
    AvailableSpace_ -= size;
}

i64 TChunkLocationBase::GetUsedSpace() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return UsedSpace_.load();
}

i64 TChunkLocationBase::GetAvailableSpace() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    if (!IsEnabled()) {
        return 0;
    }

    i64 availableSpace = 0;
    try {
        auto statistics = NFS::GetDiskSpaceStatistics(GetPath());
        availableSpace = statistics.AvailableSpace + GetAdditionalSpace();

        i64 remainingQuota = std::max(static_cast<i64>(0), GetQuota() - GetUsedSpace());
        availableSpace = std::min(availableSpace, remainingQuota);
        AvailableSpace_.store(availableSpace);

        return availableSpace;
    } catch (const std::exception& ex) {
        auto error = TError("Failed to compute available space")
            << ex;
        const_cast<TChunkLocationBase*>(this)->ScheduleDisable(error);
        return 0;
    }
}

TError TChunkLocationBase::GetLocationDisableError() const
{
    return LocationDisabledAlert_.Load();
}

void TChunkLocationBase::UpdateSessionCount(ESessionType type, int delta)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    if (!IsEnabled()) {
        return;
    }

    PerTypeSessionCount_[type] += delta;
}

int TChunkLocationBase::GetSessionCount(ESessionType type) const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return PerTypeSessionCount_[type];
}

int TChunkLocationBase::GetSessionCount() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    int result = 0;
    for (const auto& count : PerTypeSessionCount_) {
        result += count.load();
    }
    return result;
}

void TChunkLocationBase::UpdateChunkCount(int delta)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    ChunkCount_ += delta;
}

int TChunkLocationBase::GetChunkCount() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return ChunkCount_;
}

TString TChunkLocationBase::GetChunkPath(TChunkId chunkId) const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return NFS::CombinePaths(GetPath(), GetRelativeChunkPath(chunkId));
}

void TChunkLocationBase::RemoveChunkFilesPermanently(TChunkId chunkId)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    try {
        YT_LOG_DEBUG("Started removing chunk files (ChunkId: %v)", chunkId);

        auto partNames = GetChunkPartNames(chunkId);
        auto directory = NFS::GetDirectoryName(GetChunkPath(chunkId));

        for (const auto& name : partNames) {
            auto fileName = NFS::CombinePaths(directory, name);
            if (NFS::Exists(fileName)) {
                NFS::Remove(fileName);
            }
        }

        YT_LOG_DEBUG("Finished removing chunk files (ChunkId: %v)", chunkId);

        UnlockChunk(chunkId);
    } catch (const std::exception& ex) {
        auto error = TError(
            NChunkClient::EErrorCode::IOError,
            "Error removing chunk %v",
            chunkId)
            << ex;
        ScheduleDisable(error);
    }
}

void TChunkLocationBase::RemoveChunkFiles(TChunkId chunkId, bool /*force*/)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    RemoveChunkFilesPermanently(chunkId);
}

TString TChunkLocationBase::GetRelativeChunkPath(TChunkId chunkId)
{
    int hashByte = chunkId.Parts32[0] & 0xff;
    return NFS::CombinePaths(Format("%02x", hashByte), ToString(chunkId));
}

void TChunkLocationBase::ForceHashDirectories(const TString& rootPath)
{
    for (int hashByte = 0; hashByte <= 0xff; ++hashByte) {
        auto hashDirectory = Format("%02x", hashByte);
        NFS::MakeDirRecursive(NFS::CombinePaths(rootPath, hashDirectory), ChunkFilesPermissions);
    }
}

void TChunkLocationBase::ValidateWritable()
{
    NFS::MakeDirRecursive(GetPath(), ChunkFilesPermissions);

    // Run first health check before to sort out read-only drives.
    HealthChecker_->RunCheck();
}

void TChunkLocationBase::InitializeCellId()
{
    auto cellIdPath = CombinePaths(GetPath(), TString(CellIdFileName));
    auto expectedCellId = CellId_;

    if (NFS::Exists(cellIdPath)) {
        TUnbufferedFileInput file(cellIdPath);
        auto cellIdString = file.ReadAll();
        TCellId cellId;
        if (!TCellId::FromString(cellIdString, &cellId)) {
            THROW_ERROR_EXCEPTION(
                "Failed to parse cell id %Qv",
                cellIdString);
        }

        if (cellId != expectedCellId) {
            THROW_ERROR_EXCEPTION(
                "Wrong cell id: expected %v, found %v",
                expectedCellId,
                cellId);
        }
    } else {
        YT_LOG_INFO("Cell id file is not found, creating");
        TFile file(cellIdPath, CreateAlways | WrOnly | Seq | CloseOnExec);
        TUnbufferedFileOutput output(file);
        output.Write(ToString(expectedCellId));
    }
}

void TChunkLocationBase::InitializeUuid()
{
    auto uuidPath = CombinePaths(GetPath(), TString(ChunkLocationUuidFileName));

    auto uuidResetPath = CombinePaths(GetPath(), TString(ChunkLocationUuidResetFileName));
    if (StaticConfig_->ResetUuid && !Exists(uuidResetPath)) {
        TFile file(uuidResetPath, CreateAlways | WrOnly | Seq | CloseOnExec);

        if (Exists(uuidPath)) {
            Remove(uuidPath);
        }
    }

    if (Exists(uuidPath)) {
        TUnbufferedFileInput file(uuidPath);
        auto uuidString = file.ReadAll();
        if (!TCellId::FromString(uuidString, &Uuid_)) {
            THROW_ERROR_EXCEPTION(
                "Failed to parse chunk location uuid %Qv",
                uuidString);
        }
    } else {
        do {
            Uuid_ = TChunkLocationUuid::Create();
        } while (Uuid_ == EmptyChunkLocationUuid || Uuid_ == InvalidChunkLocationUuid);
        YT_LOG_INFO(
            "Chunk location uuid file is not found, creating (LocationUuid: %v)",
            Uuid_);
        TFile file(uuidPath, CreateAlways | WrOnly | Seq | CloseOnExec);
        TUnbufferedFileOutput output(file);
        output.Write(ToString(Uuid_));
    }
}

bool TChunkLocationBase::IsSick() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return IOEngine_->IsSick();
}

TLockedChunkGuard TChunkLocationBase::TryLockChunk(TChunkId chunkId)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto guard = Guard(LockedChunksLock_);

    if (!LockedChunkIds_.insert(chunkId).second) {
        YT_LOG_DEBUG(
            "Chunk is already locked (ChunkId: %v)",
            chunkId);
        return {};
    }
    YT_LOG_DEBUG(
        "Chunk locked (ChunkId: %v)",
        chunkId);
    return {this, chunkId};
}

void TChunkLocationBase::UnlockChunk(TChunkId chunkId)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto guard = Guard(LockedChunksLock_);
    if (LockedChunkIds_.erase(chunkId) == 0) {
        if (TypeFromId(chunkId) != EObjectType::NbdChunk) {
            YT_LOG_ALERT(
                "Attempt to unlock a non-locked chunk (ChunkId: %v)",
                chunkId);
        }
    } else {
        YT_LOG_DEBUG(
            "Chunk unlocked (ChunkId: %v)",
            chunkId);
    }
}

void TChunkLocationBase::UnlockChunkLocks()
{
    YT_ASSERT_INVOKER_AFFINITY(GetAuxPoolInvoker());

    auto state = GetState();
    YT_LOG_FATAL_IF(
        state != ELocationState::Disabling,
        "Remove location chunk locks should be called when state is equal to ELocationState::Disabling");

    auto guard = Guard(LockedChunksLock_);
    LockedChunkIds_.clear();
}

void TChunkLocationBase::OnHealthCheckFailed(const TError& error)
{
    ScheduleDisable(error);
}

bool TChunkLocationBase::IsLocationDiskOK() const
{
    return LocationDiskFailedAlert_.Load().IsOK();
}

void TChunkLocationBase::MarkLocationDiskFailed()
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    YT_LOG_WARNING(
        "Disk with store location failed (LocationUuid: %v, LocationIndex: %v, DiskName: %v)",
        GetUuid(),
        GetIndex(),
        StaticConfig_->DeviceName);

    LocationDiskFailedAlert_.Store(
        TError(
            NChunkClient::EErrorCode::LocationDiskFailed,
            "Disk of chunk location is marked as failed")
            << TErrorAttribute("location_uuid", GetUuid())
            << TErrorAttribute("location_path", GetPath())
            << TErrorAttribute("location_disk", StaticConfig_->DeviceName));
}

void TChunkLocationBase::MarkLocationDiskWaitingReplacement()
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    LocationDiskFailedAlert_.Store(
        TError(
            NChunkClient::EErrorCode::LocationDiskWaitingReplacement,
            "Disk of chunk location is waiting replacement")
            << TErrorAttribute("location_uuid", GetUuid())
            << TErrorAttribute("location_path", GetPath())
            << TErrorAttribute("location_disk", StaticConfig_->DeviceName));
}

void TChunkLocationBase::MarkUninitializedLocationDisabled(const TError& error)
{
    if (!ChangeState(ELocationState::Disabling, ELocationState::Enabling)) {
        return;
    }

    LocationDisabledAlert_.Store(TError(
        NChunkClient::EErrorCode::LocationDisabled,
        "Chunk location at %v is disabled", GetPath())
        << TErrorAttribute("location_uuid", GetUuid())
        << TErrorAttribute("location_path", GetPath())
        << TErrorAttribute("location_disk", StaticConfig_->DeviceName)
        << error);

    AvailableSpace_.store(0);
    UsedSpace_.store(0);
    for (auto& count : PerTypeSessionCount_) {
        count.store(0);
    }
    ChunkCount_.store(0);

    ChangeState(ELocationState::Disabled, ELocationState::Disabling);
}

i64 TChunkLocationBase::GetAdditionalSpace() const
{
    return 0;
}

bool TChunkLocationBase::ShouldSkipFileName(const std::string& fileName) const
{
    return
        fileName == CellIdFileName ||
        fileName == ChunkLocationUuidFileName ||
        fileName == ChunkLocationUuidResetFileName;
}

std::vector<TChunkDescriptor> TChunkLocationBase::DoScan()
{
    YT_LOG_INFO("Started scanning location");

    NFS::CleanTempFiles(GetPath());
    ForceHashDirectories(GetPath());

    THashSet<TChunkId> chunkIds;
    {
        // Enumerate files under the location's directory.
        // Note that these also include trash files but the latter are explicitly skipped.
        auto fileNames = NFS::EnumerateFiles(GetPath(), std::numeric_limits<int>::max());
        for (const auto& fileName : fileNames) {
            if (ShouldSkipFileName(fileName)) {
                continue;
            }

            TChunkId chunkId;
            auto bareFileName = NFS::GetFileNameWithoutExtension(fileName);
            if (!TChunkId::FromString(bareFileName, &chunkId)) {
                YT_LOG_ERROR("Unrecognized file in location directory (FileName: %v)", fileName);
                continue;
            }

            chunkIds.insert(chunkId);
        }
    }

    // Construct the list of chunk descriptors.
    // Also "repair" half-alive chunks (e.g. those having some of their essential parts missing)
    // by moving them into trash.
    std::vector<TChunkDescriptor> descriptors;
    for (auto chunkId : chunkIds) {
        if (TypeFromId(DecodeChunkId(chunkId).Id) == EObjectType::NbdChunk) {
            YT_LOG_DEBUG("Removing left over NBD chunk (ChunkId: %v)", chunkId);
            RemoveChunkFiles(chunkId, /*force*/ true);
            continue;
        }
        auto optionalDescriptor = RepairChunk(chunkId);
        if (optionalDescriptor) {
            descriptors.push_back(*optionalDescriptor);
        }
    }

    YT_LOG_INFO(
        "Finished scanning location (CountChunk: %v)",
        descriptors.size());

    return descriptors;
}

void TChunkLocationBase::DoStart()
{
    HealthChecker_->SubscribeFailed(BIND(&TChunkLocationBase::OnHealthCheckFailed, Unretained(this)));
    HealthChecker_->Start();
}

TFuture<void> TChunkLocationBase::SynchronizeActions()
{
    YT_ASSERT_INVOKER_AFFINITY(GetAuxPoolInvoker());

    auto state = GetState();
    YT_LOG_FATAL_IF(
        state != ELocationState::Disabling,
        "Synchronization of actions should be called when state is equal to ELocationState::Disabling");

    std::vector<TFuture<void>> futures;
    {
        auto actionsGuard = Guard(ActionsContainerLock_);
        futures = {Actions_.begin(), Actions_.end()};
    }

    return AllSet(futures)
        .AsVoid()
        .Apply(BIND([=, this, this_ = MakeStrong(this)] {
            // All actions with this location ended here.
            auto actionsGuard = Guard(ActionsContainerLock_);
            YT_VERIFY(Actions_.empty());
        }));
}

void TChunkLocationBase::CreateDisableLockFile(const TError& reason)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto state = GetState();
    YT_LOG_FATAL_IF(
        state != ELocationState::Disabling,
        "Disable lock file should be created when state is equal to ELocationState::Disabling");

    // Save the reason in a file and exit.
    // Location will be disabled during the scan in the restart process.
    auto lockFilePath = NFS::CombinePaths(GetPath(), DisabledLockFileName);

    auto briefConfig = GetBriefConfigCallback_();

    try {
        TFile file(lockFilePath, CreateAlways | WrOnly | Seq | CloseOnExec);
        TUnbufferedFileOutput fileOutput(file);
        fileOutput << ConvertToYsonString(reason, NYson::EYsonFormat::Pretty).AsStringBuf();
    } catch (const std::exception& ex) {
        if (briefConfig.AbortOnLocationDisabled) {
            YT_LOG_ERROR(ex, "Error creating location lock file; aborting");
        } else {
            THROW_ERROR_EXCEPTION("Error creating location lock file; aborting")
                << ex;
        }
    }

    if (briefConfig.AbortOnLocationDisabled) {
        YT_LOG_FATAL(reason);
    }
}

void TChunkLocationBase::ResetLocationStatistic()
{
    YT_ASSERT_INVOKER_AFFINITY(GetAuxPoolInvoker());

    AvailableSpace_.store(0);
    UsedSpace_.store(0);
    for (auto& count : PerTypeSessionCount_) {
        count.store(0);
    }
    ChunkCount_.store(0);
}

THazardPtr<TChunkLocationConfigBase> TChunkLocationBase::GetRuntimeConfig() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return RuntimeConfig_.AcquireHazard();
}

TChunkLocationConfigBasePtr TChunkLocationBase::GetStaticConfig() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return StaticConfig_;
}

void TChunkLocationBase::UpdateMediumTag(const std::string& mediumName)
{
    Profiler_.RenameDynamicTag(MediumTag_, "medium", mediumName);
}

void TChunkLocationBase::PopulateAlerts(std::vector<TError>* alerts)
{
    for (const auto* alertHolder : {&LocationDisabledAlert_, &LocationDiskFailedAlert_}) {
        if (auto alert = alertHolder->Load(); !alert.IsOK()) {
            alerts->push_back(std::move(alert));
        }
    }
}

TErrorOr<TLocationFairShareSlotPtr> TChunkLocationBase::AddFairShareQueueSlot(
    i64 size,
    std::vector<IFairShareHierarchicalSlotQueueResourcePtr> resources,
    std::vector<TFairShareHierarchyLevel<std::string>> levels)
{
    auto slotOrError = IOFairShareQueue_->EnqueueSlot(
        size,
        std::move(resources),
        std::move(levels));

    if (slotOrError.IsOK()) {
        YT_LOG_DEBUG(
            "Add new fair share slot (SlotId: %v, SlotSize: %v)",
            slotOrError.Value()->GetSlotId(),
            size);
        return New<TLocationFairShareSlot>(
            IOFairShareQueue_,
            std::move(slotOrError.Value()));
    }

    return slotOrError.Wrap();
}

NIO::IIOEngineWorkloadModelPtr TChunkLocationBase::GetIOEngineModel() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return IOEngineModel_;
}

bool TChunkLocationBase::StartDestroy()
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    if (!ChangeState(ELocationState::Destroying, ELocationState::Disabled)) {
        return false;
    }

    YT_LOG_INFO(
        "Starting location destruction (LocationUuid: %v, LocationIndex: %v, DiskName: %v)",
        GetUuid(),
        GetIndex(),
        StaticConfig_->DeviceName);
    return true;
}

bool TChunkLocationBase::FinishDestroy(
    bool destroyResult,
    const TError& reason = {})
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    if (destroyResult) {
        if (!ChangeState(ELocationState::Destroyed, ELocationState::Destroying)) {
            return false;
        }

        YT_LOG_INFO(
            "Finish location destruction (LocationUuid: %v, LocationIndex: %v, DiskName: %v)",
            GetUuid(),
            GetIndex(),
            StaticConfig_->DeviceName);
    } else {
        if (!ChangeState(ELocationState::Disabled, ELocationState::Destroying)) {
            return false;
        }

        YT_LOG_ERROR(reason, "Location destroying failed");
    }

    return true;
}

void TChunkLocationBase::Crash(const TError& reason)
{
    YT_LOG_ERROR(reason, "Error during location initialization");

    LocationDisabledAlert_.Store(
        TError(
            NChunkClient::EErrorCode::LocationCrashed,
            "Error during location initialization")
            << TErrorAttribute("location_path", GetPath())
            << TErrorAttribute("location_disk", StaticConfig_->DeviceName)
            << reason);

    ChangeState(ELocationState::Crashed);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNode
