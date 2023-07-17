#include "location.h"

#include "private.h"
#include "blob_chunk.h"
#include "blob_reader_cache.h"
#include "chunk_store.h"
#include "config.h"
#include "journal_chunk.h"
#include "journal_dispatcher.h"
#include "journal_manager.h"
#include "master_connector.h"
#include "medium_updater.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/lib/hydra_common/file_changelog.h>
#include <yt/yt/server/lib/hydra_common/private.h>

#include <yt/yt/server/lib/misc/disk_health_checker.h>
#include <yt/yt/server/lib/misc/private.h>

#include <yt/yt/server/lib/io/io_engine.h>
#include <yt/yt/server/lib/io/io_workload_model.h>
#include <yt/yt/server/lib/io/dynamic_io_engine.h>

#include <yt/yt/ytlib/chunk_client/format.h>
#include <yt/yt/ytlib/chunk_client/medium_directory_synchronizer.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/library/program/program.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/concurrency/thread_pool.h>

namespace NYT::NDataNode {

using namespace NChunkClient;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NLogging;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NHydra;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

// Others must not be able to list chunk store and chunk cache directories.
static constexpr int ChunkFilesPermissions = 0751;

// https://www.kernel.org/doc/html/latest/block/stat.html
// read sectors, write sectors, discard_sectors
// These values count the number of sectors read from, written to, or discarded from this block device.
// The “sectors” in question are the standard UNIX 512-byte sectors, not any device- or filesystem-specific block size.
static constexpr int UnixSectorSize = 512;

////////////////////////////////////////////////////////////////////////////////

TLocationPerformanceCounters::TLocationPerformanceCounters(const NProfiling::TProfiler& profiler)
{
    for (auto direction : TEnumTraits<EIODirection>::GetDomainValues()) {
        for (auto category : TEnumTraits<EIOCategory>::GetDomainValues()) {
            auto r = profiler
                .WithTag("direction", FormatEnum(direction), -1)
                .WithTag("category", FormatEnum(category), -2);

            r.AddFuncGauge("/pending_data_size", MakeStrong(this), [this, direction, category] {
                return PendingIOSize[direction][category].load();
            });

            CompletedIOSize[direction][category] = r.Counter("/blob_block_bytes");
        }
    }

    ThrottledReads = profiler.Counter("/throttled_reads");
    ThrottledWrites = profiler.Counter("/throttled_writes");

    PutBlocksWallTime = profiler.Timer("/put_blocks_wall_time");
    BlobChunkMetaReadTime = profiler.Timer("/blob_chunk_meta_read_time");

    BlobChunkWriterOpenTime = profiler.Timer("/blob_chunk_writer_open_time");
    BlobChunkWriterAbortTime = profiler.Timer("/blob_chunk_writer_abort_time");
    BlobChunkWriterCloseTime = profiler.Timer("/blob_chunk_writer_close_time");

    BlobBlockReadBytes = profiler.Counter("/blob_block_read_bytes");
    BlobBlockReadCount = profiler.Counter("/blob_block_read_count");

    for (auto category : TEnumTraits<EWorkloadCategory>::GetDomainValues()) {
        auto categoryProfiler = profiler
            .WithTag("category", FormatEnum(category), -1);

        BlobBlockReadLatencies[category] = categoryProfiler.Timer("/blob_block_read_latency");
        BlobChunkMetaReadLatencies[category] = categoryProfiler.Timer("/blob_chunk_meta_read_latency");

        {
            // Try to save Solomon resources making separate counters only for UserInteractive workload.
            const auto& selectedProfiler = (category == EWorkloadCategory::UserInteractive) ? categoryProfiler : profiler;
            BlobBlockReadSize[category] = selectedProfiler.Summary("/blob_block_read_size");
            BlobBlockReadTime[category] = selectedProfiler.Histogram(
                "/blob_block_read_time",
                TDuration::MicroSeconds(1),
                TDuration::Seconds(125));
        }
    }

    BlobBlockWriteSize = profiler.Summary("/blob_block_write_size");
    BlobBlockWriteTime = profiler.Timer("/blob_block_write_time");
    BlobBlockWriteBytes = profiler.Counter("/blob_block_write_bytes");

    JournalBlockReadSize = profiler.Summary("/journal_block_read_size");
    JournalBlockReadTime = profiler.Timer("/journal_block_read_time");
    JournalBlockReadBytes = profiler.Counter("/journal_block_read_bytes");

    JournalChunkCreateTime = profiler.Timer("/journal_chunk_create_time");
    JournalChunkOpenTime = profiler.Timer("/journal_chunk_open_time");
    JournalChunkRemoveTime = profiler.Timer("/journal_chunk_remove_time");

    for (auto type : TEnumTraits<ESessionType>::GetDomainValues()) {
        profiler.WithTag("type", FormatEnum(type)).AddFuncGauge("/session_count", MakeStrong(this), [this, type] {
            return SessionCount[type].load();
        });
    }

    UsedSpace = profiler.Gauge("/used_space");
    AvailableSpace = profiler.Gauge("/available_space");
    ChunkCount = profiler.Gauge("/chunk_count");
    Full = profiler.Gauge("/full");
}

void TLocationPerformanceCounters::ReportThrottledRead()
{
    ThrottledReads.Increment();
    LastReadThrottleTime = GetCpuInstant();
}

void TLocationPerformanceCounters::ReportThrottledWrite()
{
    ThrottledWrites.Increment();
    LastWriteThrottleTime = GetCpuInstant();
}

////////////////////////////////////////////////////////////////////////////////

TPendingIOGuard::TPendingIOGuard(
    TMemoryUsageTrackerGuard memoryGuard,
    EIODirection direction,
    EIOCategory category,
    i64 size,
    TChunkLocationPtr owner)
    : MemoryGuard_(std::move(memoryGuard))
    , Direction_(direction)
    , Category_(category)
    , Size_(size)
    , Owner_(owner)
{ }

TPendingIOGuard::TPendingIOGuard(TPendingIOGuard&& other)
{
    MoveFrom(std::move(other));
}

void TPendingIOGuard::MoveFrom(TPendingIOGuard&& other)
{
    MemoryGuard_ = std::move(other.MemoryGuard_);
    Direction_ = other.Direction_;
    Category_ = other.Category_;
    Size_ = other.Size_;
    Owner_ = std::move(other.Owner_);

    other.MemoryGuard_.Release();
    other.Size_ = 0;
    other.Owner_.Reset();
}

TPendingIOGuard::~TPendingIOGuard()
{
    Release();
}

TPendingIOGuard& TPendingIOGuard::operator=(TPendingIOGuard&& other)
{
    if (this != &other) {
        Release();
        MoveFrom(std::move(other));
    }
    return *this;
}

void TPendingIOGuard::Release()
{
    if (Owner_) {
        Owner_->DecreasePendingIOSize(Direction_, Category_, Size_);
        MemoryGuard_.Release();
        Owner_.Reset();
        Size_ = 0;
    }
}

TPendingIOGuard::operator bool() const
{
    return Owner_.operator bool();
}

////////////////////////////////////////////////////////////////////////////////

TLockedChunkGuard::TLockedChunkGuard(TChunkLocationPtr location, TChunkId chunkId)
    : Location_(std::move(location))
    , ChunkId_(chunkId)
{ }

TLockedChunkGuard::TLockedChunkGuard(TLockedChunkGuard&& other)
{
    MoveFrom(std::move(other));
}

TLockedChunkGuard::~TLockedChunkGuard()
{
    if (Location_) {
        Location_->UnlockChunk(ChunkId_);
    }
}

TLockedChunkGuard& TLockedChunkGuard::operator=(TLockedChunkGuard&& other)
{
    if (this != &other) {
        MoveFrom(std::move(other));
    }
    return *this;
}

void TLockedChunkGuard::Release()
{
    Location_.Reset();
    ChunkId_ = {};
}

void TLockedChunkGuard::MoveFrom(TLockedChunkGuard&& other)
{
    Location_ = std::move(other.Location_);
    ChunkId_ = other.ChunkId_;

    other.Location_.Reset();
    other.ChunkId_ = {};
}

TLockedChunkGuard::operator bool() const
{
    return Location_.operator bool();
}

////////////////////////////////////////////////////////////////////////////////

TChunkLocation::TChunkLocation(
    ELocationType type,
    TString id,
    TChunkLocationConfigPtr config,
    TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
    TChunkStorePtr chunkStore,
    TChunkContextPtr chunkContext,
    IChunkStoreHostPtr chunkStoreHost)
    : TDiskLocation(
        config,
        std::move(id),
        DataNodeLogger)
    , DynamicConfigManager_(std::move(dynamicConfigManager))
    , ChunkStore_(std::move(chunkStore))
    , ChunkContext_(std::move(chunkContext))
    , ChunkStoreHost_(std::move(chunkStoreHost))
    , Type_(type)
    , StaticConfig_(std::move(config))
    , ReadMemoryTracker_(ChunkStoreHost_->GetMemoryUsageTracker()->WithCategory(EMemoryCategory::PendingDiskRead))
    , WriteMemoryTracker_(ChunkStoreHost_->GetMemoryUsageTracker()->WithCategory(EMemoryCategory::PendingDiskWrite))
    , RuntimeConfig_(StaticConfig_)
    , MediumDescriptor_(TMediumDescriptor{
        .Name = StaticConfig_->MediumName
    })
{
    TTagSet tagSet;
    tagSet.AddTag({"location_type", FormatEnum(Type_)});
    tagSet.AddTag({"disk_family", StaticConfig_->DiskFamily}, -1);
    tagSet.AddTag({"medium", GetMediumName()}, -1);
    tagSet.AddTag({"location_id", Id_}, -1);
    tagSet.AddExtensionTag({"device_name", StaticConfig_->DeviceName}, -1);
    tagSet.AddExtensionTag({"device_model", StaticConfig_->DeviceModel}, -1);

    MediumTag_ = tagSet.AddDynamicTag(2);

    Profiler_ = LocationProfiler
        .WithSparse()
        .WithTags(tagSet);

    PerformanceCounters_ = New<TLocationPerformanceCounters>(Profiler_);

    MediumFlag_ = Profiler_.Gauge("/medium");
    MediumFlag_.Update(1);

    UpdateMediumTag();

    DynamicIOEngine_ = CreateDynamicIOEngine(
        StaticConfig_->IOEngineType,
        StaticConfig_->IOConfig,
        Id_,
        Profiler_,
        DataNodeLogger.WithTag("LocationId: %v", Id_));
    IOEngineModel_ = CreateIOModelInterceptor(
        Id_,
        DynamicIOEngine_,
        DataNodeLogger.WithTag("IOModel: %v", Id_));
    IOEngine_ = IOEngineModel_;

    auto diskThrottlerProfiler = GetProfiler().WithPrefix("/disk_throttler");
    for (auto kind : TEnumTraits<EChunkLocationThrottlerKind>::GetDomainValues()) {
        Throttlers_[kind] = ReconfigurableThrottlers_[kind] = CreateNamedReconfigurableThroughputThrottler(
            StaticConfig_->Throttlers[kind],
            ToString(kind),
            Logger,
            diskThrottlerProfiler);
    }
    UnlimitedInThrottler_ = CreateNamedUnlimitedThroughputThrottler(
        "UnlimitedIn",
        diskThrottlerProfiler);
    UnlimitedOutThrottler_ = CreateNamedUnlimitedThroughputThrottler(
        "UnlimitedOutThrottler",
        diskThrottlerProfiler);

    HealthChecker_ = New<TDiskHealthChecker>(
        ChunkContext_->DataNodeConfig->DiskHealthChecker,
        GetPath(),
        GetAuxPoolInvoker(),
        DataNodeLogger,
        Profiler_);

    ChunkStoreHost_->SubscribePopulateAlerts(
        BIND(&TChunkLocation::PopulateAlerts, MakeWeak(this)));
}

const NIO::IIOEnginePtr& TChunkLocation::GetIOEngine() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return IOEngine_;
}

const NIO::IIOEngineWorkloadModelPtr& TChunkLocation::GetIOEngineModel() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return IOEngineModel_;
}

THazardPtr<TChunkLocationConfig> TChunkLocation::GetRuntimeConfig() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return RuntimeConfig_.AcquireHazard();
}

void TChunkLocation::Reconfigure(TChunkLocationConfigPtr config)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TDiskLocation::Reconfigure(config);

    DynamicIOEngine_->SetType(config->IOEngineType);
    DynamicIOEngine_->Reconfigure(config->IOConfig);

    for (auto kind : TEnumTraits<EChunkLocationThrottlerKind>::GetDomainValues()) {
        ReconfigurableThrottlers_[kind]->Reconfigure(config->Throttlers[kind]);
    }

    RuntimeConfig_.Store(std::move(config));
}

ELocationType TChunkLocation::GetType() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Type_;
}

TChunkLocationUuid TChunkLocation::GetUuid() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Uuid_;
}

const TString& TChunkLocation::GetDiskFamily() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return StaticConfig_->DiskFamily;
}

TString TChunkLocation::GetMediumName() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return GetMediumDescriptor().Name;
}

TMediumDescriptor TChunkLocation::GetMediumDescriptor() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return MediumDescriptor_.Load();
}

const NProfiling::TProfiler& TChunkLocation::GetProfiler() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Profiler_;
}

TLocationPerformanceCounters& TChunkLocation::GetPerformanceCounters()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return *PerformanceCounters_;
}

const TString& TChunkLocation::GetPath() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return StaticConfig_->Path;
}

i64 TChunkLocation::GetQuota() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return StaticConfig_->Quota.value_or(std::numeric_limits<i64>::max());
}

double TChunkLocation::GetIOWeight() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return StaticConfig_->IOWeight;
}

i64 TChunkLocation::GetCoalescedReadMaxGapSize() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return GetRuntimeConfig()->CoalescedReadMaxGapSize;
}

const IInvokerPtr& TChunkLocation::GetAuxPoolInvoker()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return IOEngine_->GetAuxPoolInvoker();
}

std::vector<TChunkDescriptor> TChunkLocation::Scan()
{
    VERIFY_INVOKER_AFFINITY(GetAuxPoolInvoker());
    YT_VERIFY(GetState() == ELocationState::Enabling);

    try {
        ValidateLockFile();
        ValidateMinimumSpace();
        ValidateWritable();
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Location disabled");
        MarkUninitializedLocationDisabled(ex);
        return {};
    }

    // Be optimistic and assume everything will be OK.
    // Also Disable requires State_ to be Enabled.
    ChangeState(ELocationState::Enabled);
    LocationDisabledAlert_.Store(TError());

    try {
        return DoScan();
    } catch (const std::exception& ex) {
        ScheduleDisable(TError("Location scan failed") << ex);
        return {};
    }
}

void TChunkLocation::Start()
{
    try {
        InitializeCellId();
        InitializeUuid();
    } catch (const std::exception& ex) {
        Crash(TError("Location initialize failed") << ex);
        return;
    }

    if (!IsEnabled()) {
        return;
    }

    try {
        DoStart();
    } catch (const std::exception& ex) {
        ScheduleDisable(TError("Location start failed") << ex);
    }
}

bool TChunkLocation::ShouldPublish() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto state = GetState();

    return state == ELocationState::Enabled ||
        (ChunkStore_->ShouldPublishDisabledLocations() &&
            GetUuid() != InvalidChunkLocationUuid &&
            GetUuid() != EmptyChunkLocationUuid);
}

bool TChunkLocation::StartDestroy()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!ChangeState(ELocationState::Destroying, ELocationState::Disabled)) {
        return false;
    }

    YT_LOG_INFO("Starting location destruction (LocationUuid: %v, DiskName: %v)",
        GetUuid(),
        StaticConfig_->DeviceName);

    LocationDiskFailedAlert_.Store(
        TError(NChunkClient::EErrorCode::LocationDiskWaitingReplacement,
            "Disk of chunk location is waiting replacement")
            << TErrorAttribute("location_uuid", GetUuid())
            << TErrorAttribute("location_path", GetPath())
            << TErrorAttribute("location_disk", StaticConfig_->DeviceName));
    return true;
}

bool TChunkLocation::FinishDestroy(
    bool destroyResult,
    const TError& reason = {})
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (destroyResult) {
        if (!ChangeState(ELocationState::Destroyed, ELocationState::Destroying)) {
            return false;
        }

        YT_LOG_INFO("Finish location destruction (LocationUuid: %v, DiskName: %v)",
            GetUuid(),
            StaticConfig_->DeviceName);
    } else {
        if (!ChangeState(ELocationState::Disabled, ELocationState::Destroying)) {
            return false;
        }

        YT_LOG_ERROR(reason, "Location destroying failed");
    }

    return true;
}

bool TChunkLocation::OnDiskRepaired()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!ChangeState(ELocationState::Disabled, ELocationState::Destroyed)) {
        return false;
    }

    LocationDiskFailedAlert_.Store(TError());
    return true;
}

bool TChunkLocation::Resurrect()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!ChangeState(ELocationState::Enabling, ELocationState::Disabled)) {
        return false;
    }

    BIND([=, this, this_ = MakeStrong(this)] () {
        try {
            // Remove disabled lock file if exists.
            auto lockFilePath = NFS::CombinePaths(GetPath(), DisabledLockFileName);

            if (NFS::Exists(lockFilePath)) {
                NFS::Remove(lockFilePath);
            }

            WaitFor(ChunkStore_->InitializeLocation(MakeStrong(dynamic_cast<TStoreLocation*>(this))))
                .ThrowOnError();
            ChunkStoreHost_->ScheduleMasterHeartbeat();
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error during location resurrection");

            ChangeState(ELocationState::Disabled, ELocationState::Enabling);
        }
    })
        .AsyncVia(GetAuxPoolInvoker())
        .Run();

    return true;
}

void TChunkLocation::Crash(const TError& reason)
{
    YT_LOG_ERROR(reason, "Error during location initialization");

    LocationDisabledAlert_.Store(
        TError(NChunkClient::EErrorCode::LocationCrashed,
            "Error during location initialization")
            << TErrorAttribute("location_path", GetPath())
            << TErrorAttribute("location_disk", StaticConfig_->DeviceName)
            << reason);

    ChangeState(ELocationState::Crashed);
}

void TChunkLocation::UpdateUsedSpace(i64 size)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!IsEnabled()) {
        return;
    }

    UsedSpace_ += size;
    AvailableSpace_ -= size;
}

i64 TChunkLocation::GetUsedSpace() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return UsedSpace_.load();
}

i64 TChunkLocation::GetAvailableSpace() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!IsEnabled()) {
        return 0;
    }

    i64 availableSpace = 0;
    try {
        auto statistics = NFS::GetDiskSpaceStatistics(GetPath());
        availableSpace = statistics.AvailableSpace + GetAdditionalSpace();
    } catch (const std::exception& ex) {
        auto error = TError("Failed to compute available space")
            << ex;
        const_cast<TChunkLocation*>(this)->ScheduleDisable(error);
    }

    i64 remainingQuota = std::max(static_cast<i64>(0), GetQuota() - GetUsedSpace());
    availableSpace = std::min(availableSpace, remainingQuota);
    AvailableSpace_.store(availableSpace);

    return availableSpace;
}

const ITypedNodeMemoryTrackerPtr& TChunkLocation::GetReadMemoryTracker() const
{
    return ReadMemoryTracker_;
}

const ITypedNodeMemoryTrackerPtr& TChunkLocation::GetWriteMemoryTracker() const
{
    return WriteMemoryTracker_;
}

i64 TChunkLocation::GetPendingIOSize(
    EIODirection direction,
    const TWorkloadDescriptor& workloadDescriptor) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto category = ToIOCategory(workloadDescriptor);
    return PerformanceCounters_->PendingIOSize[direction][category].load();
}

i64 TChunkLocation::GetMaxPendingIOSize(EIODirection direction) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    i64 result = 0;
    for (auto category : TEnumTraits<EIOCategory>::GetDomainValues()) {
        result = std::max(result, PerformanceCounters_->PendingIOSize[direction][category].load());
    }
    return result;
}

TPendingIOGuard TChunkLocation::AcquirePendingIO(
    TMemoryUsageTrackerGuard memoryGuard,
    EIODirection direction,
    const TWorkloadDescriptor& workloadDescriptor,
    i64 delta)
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_ASSERT(delta >= 0);
    auto category = ToIOCategory(workloadDescriptor);
    UpdatePendingIOSize(direction, category, delta);
    return TPendingIOGuard(
        std::move(memoryGuard),
        direction,
        category,
        delta,
        this);
}

EIOCategory TChunkLocation::ToIOCategory(const TWorkloadDescriptor& workloadDescriptor)
{
    switch (workloadDescriptor.Category) {
        case EWorkloadCategory::Idle:
        case EWorkloadCategory::SystemReplication:
        case EWorkloadCategory::SystemTabletCompaction:
        case EWorkloadCategory::SystemTabletPartitioning:
        case EWorkloadCategory::SystemTabletPreload:
        case EWorkloadCategory::SystemTabletStoreFlush:
        case EWorkloadCategory::SystemArtifactCacheDownload:
        case EWorkloadCategory::UserBatch:
            return EIOCategory::Batch;

        case EWorkloadCategory::UserRealtime:
        case EWorkloadCategory::SystemTabletLogging:
            return EIOCategory::Realtime;

        case EWorkloadCategory::SystemRepair:
            return EIOCategory::Repair;

        case EWorkloadCategory::SystemTabletRecovery:
        case EWorkloadCategory::UserInteractive:
            return EIOCategory::Interactive;

        default:
            // Graceful fallback for possible future extensions of categories.
            return EIOCategory::Batch;
    }
}

void TChunkLocation::DecreasePendingIOSize(
    EIODirection direction,
    EIOCategory category,
    i64 delta)
{
    VERIFY_THREAD_AFFINITY_ANY();

    UpdatePendingIOSize(direction, category, -delta);
}

void TChunkLocation::UpdatePendingIOSize(
    EIODirection direction,
    EIOCategory category,
    i64 delta)
{
    VERIFY_THREAD_AFFINITY_ANY();

    i64 result = PerformanceCounters_->PendingIOSize[direction][category].fetch_add(delta) + delta;
    YT_LOG_TRACE("Pending IO size updated (Direction: %v, Category: %v, PendingSize: %v, Delta: %v)",
        direction,
        category,
        result,
        delta);
}

void TChunkLocation::IncreaseCompletedIOSize(
    EIODirection direction,
    const TWorkloadDescriptor& workloadDescriptor,
    i64 delta)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto category = ToIOCategory(workloadDescriptor);
    PerformanceCounters_->CompletedIOSize[direction][category].Increment(delta);
}

void TChunkLocation::UpdateSessionCount(ESessionType type, int delta)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!IsEnabled()) {
        return;
    }

    PerTypeSessionCount_[type] += delta;
}

int TChunkLocation::GetSessionCount(ESessionType type) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return PerTypeSessionCount_[type];
}

int TChunkLocation::GetSessionCount() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    int result = 0;
    for (const auto& count : PerTypeSessionCount_) {
        result += count.load();
    }
    return result;
}

void TChunkLocation::UpdateChunkCount(int delta)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!IsEnabled()) {
        return;
    }

    ChunkCount_ += delta;
}

int TChunkLocation::GetChunkCount() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return ChunkCount_;
}

TString TChunkLocation::GetChunkPath(TChunkId chunkId) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return NFS::CombinePaths(GetPath(), GetRelativeChunkPath(chunkId));
}

void TChunkLocation::RemoveChunkFilesPermanently(TChunkId chunkId)
{
    VERIFY_THREAD_AFFINITY_ANY();

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

void TChunkLocation::RemoveChunkFiles(TChunkId chunkId, bool /*force*/)
{
    VERIFY_THREAD_AFFINITY_ANY();

    RemoveChunkFilesPermanently(chunkId);
}

const IThroughputThrottlerPtr& TChunkLocation::GetInThrottler(const TWorkloadDescriptor& descriptor) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    switch (descriptor.Category) {
        case EWorkloadCategory::SystemRepair:
            return Throttlers_[EChunkLocationThrottlerKind::RepairIn];

        case EWorkloadCategory::SystemReplication:
            return Throttlers_[EChunkLocationThrottlerKind::ReplicationIn];

        case EWorkloadCategory::SystemTabletLogging:
            return Throttlers_[EChunkLocationThrottlerKind::TabletLoggingIn];

        case EWorkloadCategory::SystemTabletCompaction:
        case EWorkloadCategory::SystemTabletPartitioning:
            return Throttlers_[EChunkLocationThrottlerKind::TabletCompactionAndPartitioningIn];

        case EWorkloadCategory::SystemTabletSnapshot:
            return Throttlers_[EChunkLocationThrottlerKind::TabletSnapshotIn];

        case EWorkloadCategory::SystemTabletStoreFlush:
            return Throttlers_[EChunkLocationThrottlerKind::TabletStoreFlushIn];

        default:
            return UnlimitedInThrottler_;
    }
}

const IThroughputThrottlerPtr& TChunkLocation::GetOutThrottler(const TWorkloadDescriptor& descriptor) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    switch (descriptor.Category) {
        case EWorkloadCategory::SystemReplication:
            return Throttlers_[EChunkLocationThrottlerKind::ReplicationOut];

        case EWorkloadCategory::SystemRepair:
            return Throttlers_[EChunkLocationThrottlerKind::RepairOut];

        case EWorkloadCategory::SystemTabletCompaction:
        case EWorkloadCategory::SystemTabletPartitioning:
            return Throttlers_[EChunkLocationThrottlerKind::TabletCompactionAndPartitioningOut];

        case EWorkloadCategory::SystemTabletLogging:
            return Throttlers_[EChunkLocationThrottlerKind::TabletLoggingOut];

        case EWorkloadCategory::SystemTabletPreload:
            return Throttlers_[EChunkLocationThrottlerKind::TabletPreloadOut];

        case EWorkloadCategory::SystemTabletRecovery:
            return Throttlers_[EChunkLocationThrottlerKind::TabletRecoveryOut];

        default:
            return UnlimitedOutThrottler_;
    }
}

bool TChunkLocation::IsReadThrottling() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto time = PerformanceCounters_->LastReadThrottleTime.load();
    auto config = GetRuntimeConfig();
    return GetCpuInstant() < time + 2 * DurationToCpuDuration(config->ThrottleDuration);
}

bool TChunkLocation::IsWriteThrottling() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto time = PerformanceCounters_->LastWriteThrottleTime.load();
    auto config = GetRuntimeConfig();
    return GetCpuInstant() < time + 2 * DurationToCpuDuration(config->ThrottleDuration);
}

TString TChunkLocation::GetRelativeChunkPath(TChunkId chunkId)
{
    int hashByte = chunkId.Parts32[0] & 0xff;
    return NFS::CombinePaths(Format("%02x", hashByte), ToString(chunkId));
}

void TChunkLocation::ForceHashDirectories(const TString& rootPath)
{
    for (int hashByte = 0; hashByte <= 0xff; ++hashByte) {
        auto hashDirectory = Format("%02x", hashByte);
        NFS::MakeDirRecursive(NFS::CombinePaths(rootPath, hashDirectory), ChunkFilesPermissions);
    }
}

void TChunkLocation::ValidateWritable()
{
    NFS::MakeDirRecursive(GetPath(), ChunkFilesPermissions);

    // Run first health check before to sort out read-only drives.
    WaitFor(HealthChecker_->RunCheck())
        .ThrowOnError();
}

void TChunkLocation::InitializeCellId()
{
    auto cellIdPath = NFS::CombinePaths(GetPath(), CellIdFileName);
    auto expectedCellId = ChunkStoreHost_->GetCellId();

    if (NFS::Exists(cellIdPath)) {
        TUnbufferedFileInput file(cellIdPath);
        auto cellIdString = file.ReadAll();
        TCellId cellId;
        if (!TCellId::FromString(cellIdString, &cellId)) {
            THROW_ERROR_EXCEPTION("Failed to parse cell id %Qv",
                cellIdString);
        }

        if (cellId != expectedCellId) {
            THROW_ERROR_EXCEPTION("Wrong cell id: expected %v, found %v",
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

void TChunkLocation::InitializeUuid()
{
    auto uuidPath = NFS::CombinePaths(GetPath(), ChunkLocationUuidFileName);

    auto uuidResetPath = NFS::CombinePaths(GetPath(), ChunkLocationUuidResetFileName);
    if (StaticConfig_->ResetUuid && !NFS::Exists(uuidResetPath)) {
        TFile file(uuidResetPath, CreateAlways | WrOnly | Seq | CloseOnExec);

        if (NFS::Exists(uuidPath)) {
            NFS::Remove(uuidPath);
        }
    }

    if (NFS::Exists(uuidPath)) {
        TUnbufferedFileInput file(uuidPath);
        auto uuidString = file.ReadAll();
        if (!TCellId::FromString(uuidString, &Uuid_)) {
            THROW_ERROR_EXCEPTION("Failed to parse chunk location uuid %Qv",
                uuidString);
        }
    } else {
        do {
            Uuid_ = TChunkLocationUuid::Create();
        } while (Uuid_ == EmptyChunkLocationUuid || Uuid_ == InvalidChunkLocationUuid);
        YT_LOG_INFO("Chunk location uuid file is not found, creating (LocationUuid: %v)",
            Uuid_);
        TFile file(uuidPath, CreateAlways | WrOnly | Seq | CloseOnExec);
        TUnbufferedFileOutput output(file);
        output.Write(ToString(Uuid_));
    }
}

std::tuple<bool, i64> TChunkLocation::CheckReadThrottling(
    const TWorkloadDescriptor& workloadDescriptor,
    bool incrementCounter) const
{
    auto readQueueSize =
        GetPendingIOSize(EIODirection::Read, workloadDescriptor) +
        GetOutThrottler(workloadDescriptor)->GetQueueTotalAmount();
    bool throttled =
        readQueueSize > GetReadThrottlingLimit() ||
        ReadMemoryTracker_->IsExceeded();
    if (throttled && incrementCounter) {
        ReportThrottledRead();
    }
    return {throttled, readQueueSize};
}

void TChunkLocation::ReportThrottledRead() const
{
    PerformanceCounters_->ReportThrottledRead();
}

bool TChunkLocation::CheckWriteThrottling(
    const TWorkloadDescriptor& workloadDescriptor,
    bool incrementCounter) const
{
    bool throttled =
        GetPendingIOSize(EIODirection::Write, workloadDescriptor) > GetWriteThrottlingLimit() ||
        WriteMemoryTracker_->IsExceeded();
    if (throttled && incrementCounter) {
        ReportThrottledWrite();
    }
    return throttled;
}

void TChunkLocation::ReportThrottledWrite() const
{
    PerformanceCounters_->ReportThrottledWrite();
}

i64 TChunkLocation::GetReadThrottlingLimit() const
{
    const auto& config = ChunkContext_->DataNodeConfig;
    auto limit = DynamicConfigManager_->GetConfig()->DataNode->DiskReadThrottlingLimit;
    return limit.value_or(config->DiskReadThrottlingLimit);
}

i64 TChunkLocation::GetWriteThrottlingLimit() const
{
    const auto& config = ChunkContext_->DataNodeConfig;
    auto limit = DynamicConfigManager_->GetConfig()->DataNode->DiskWriteThrottlingLimit;
    return limit.value_or(config->DiskWriteThrottlingLimit);
}

bool TChunkLocation::IsSick() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return IOEngine_->IsSick();
}

TLockedChunkGuard TChunkLocation::TryLockChunk(TChunkId chunkId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = Guard(LockedChunksLock_);
    if (!LockedChunkIds_.insert(chunkId).second) {
        return {};
    }
    YT_LOG_DEBUG("Chunk locked (ChunkId: %v)",
        chunkId);
    return {this, chunkId};
}

void TChunkLocation::UnlockChunk(TChunkId chunkId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = Guard(LockedChunksLock_);
    if (LockedChunkIds_.erase(chunkId) == 0) {
        YT_LOG_ALERT("Attempt to unlock a non-locked chunk (ChunkId: %v)",
            chunkId);
    } else {
        YT_LOG_DEBUG("Chunk unlocked (ChunkId: %v)",
            chunkId);
    }
}

void TChunkLocation::OnHealthCheckFailed(const TError& error)
{
    ScheduleDisable(error);
}

bool TChunkLocation::IsLocationDiskOK() const
{
    return LocationDiskFailedAlert_.Load().IsOK();
}

void TChunkLocation::MarkLocationDiskFailed()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!IsLocationDiskOK()) {
        // do nothing
        return;
    }

    YT_LOG_WARNING("Disk with store location failed (LocationUuid: %v, DiskName: %v)",
        GetUuid(),
        StaticConfig_->DeviceName);

    LocationDiskFailedAlert_.Store(
        TError(NChunkClient::EErrorCode::LocationDiskFailed,
            "Disk of chunk location is marked as failed")
            << TErrorAttribute("location_uuid", GetUuid())
            << TErrorAttribute("location_path", GetPath())
            << TErrorAttribute("location_disk", StaticConfig_->DeviceName));
}

void TChunkLocation::MarkUninitializedLocationDisabled(const TError& error)
{
    if (!ChangeState(ELocationState::Disabling, ELocationState::Enabling)) {
        return;
    }

    LocationDisabledAlert_.Store(TError("Chunk location at %v is disabled", GetPath())
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

    Profiler_
        .WithTag("error_code", ToString(static_cast<int>(error.GetNonTrivialCode())))
        .AddFuncGauge("/disabled", MakeStrong(this), [] { return 1.0; });

    ChangeState(ELocationState::Disabled, ELocationState::Disabling);
}

i64 TChunkLocation::GetAdditionalSpace() const
{
    return 0;
}

bool TChunkLocation::ShouldSkipFileName(const TString& fileName) const
{
    return
        fileName == CellIdFileName ||
        fileName == ChunkLocationUuidFileName ||
        fileName == ChunkLocationUuidResetFileName;
}

std::vector<TChunkDescriptor> TChunkLocation::DoScan()
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
        auto optionalDescriptor = RepairChunk(chunkId);
        if (optionalDescriptor) {
            descriptors.push_back(*optionalDescriptor);
        }
    }

    YT_LOG_INFO("Finished scanning location (CountChunk: %v)",
        descriptors.size());

    return descriptors;
}

void TChunkLocation::DoStart()
{
    HealthChecker_->SubscribeFailed(BIND(&TChunkLocation::OnHealthCheckFailed, Unretained(this)));
    HealthChecker_->Start();
}

void TChunkLocation::SubscribeDiskCheckFailed(const TCallback<void(const TError&)> callback)
{
    HealthChecker_->SubscribeFailed(callback);
}

void TChunkLocation::UpdateMediumTag()
{
    LocationProfiler.RenameDynamicTag(MediumTag_, "medium", GetMediumName());
}

void TChunkLocation::UpdateMediumDescriptor(const NChunkClient::TMediumDescriptor& newDescriptor, bool onInitialize)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YT_VERIFY(newDescriptor.Index != GenericMediumIndex);

    auto oldDescriptor = MediumDescriptor_.Exchange(newDescriptor);

    if (newDescriptor == oldDescriptor) {
        return;
    }

    UpdateMediumTag();
    if (ChunkStore_ && !onInitialize && newDescriptor.Index != oldDescriptor.Index) {
        ChunkStore_->ChangeLocationMedium(this, oldDescriptor.Index);
    }

    YT_LOG_INFO("Location medium descriptor %v (LocationId: %v, LocationUuid: %v, MediumName: %v, MediumIndex: %v, Priority: %v)",
        onInitialize ? "set" : "changed",
        GetId(),
        GetUuid(),
        newDescriptor.Name,
        newDescriptor.Index,
        newDescriptor.Priority);
}

void TChunkLocation::PopulateAlerts(std::vector<TError>* alerts)
{
    for (const auto* alertHolder : {&LocationDisabledAlert_, &LocationDiskFailedAlert_}) {
        if (auto alert = alertHolder->Load(); !alert.IsOK()) {
            alerts->push_back(std::move(alert));
        }
    }
}

TFuture<void> TChunkLocation::SynchronizeActions()
{
    VERIFY_INVOKER_AFFINITY(GetAuxPoolInvoker());

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
        .Apply(BIND([=, this, this_ = MakeStrong(this)] () {
            // All actions with this location ended here.
            auto actionsGuard = Guard(ActionsContainerLock_);
            YT_VERIFY(Actions_.empty());
        }));
}

void TChunkLocation::CreateDisableLockFile(const TError& reason)
{
    VERIFY_INVOKER_AFFINITY(GetAuxPoolInvoker());

    auto state = GetState();
    YT_LOG_FATAL_IF(
        state != ELocationState::Disabling,
        "Disable lock file should be created when state is equal to ELocationState::Disabling");

    // Save the reason in a file and exit.
    // Location will be disabled during the scan in the restart process.
    auto lockFilePath = NFS::CombinePaths(GetPath(), DisabledLockFileName);
    auto dynamicConfig = DynamicConfigManager_->GetConfig()->DataNode;

    try {
        TFile file(lockFilePath, CreateAlways | WrOnly | Seq | CloseOnExec);
        TUnbufferedFileOutput fileOutput(file);
        fileOutput << ConvertToYsonString(reason, NYson::EYsonFormat::Pretty).AsStringBuf();
    } catch (const std::exception& ex) {
        if (dynamicConfig->AbortOnLocationDisabled) {
            YT_LOG_ERROR(ex, "Error creating location lock file; aborting");
            TProgram::Abort(EProgramExitCode::ProgramError);
        } else {
            THROW_ERROR_EXCEPTION("Error creating location lock file; aborting")
                << ex;
        }
    }

    if (dynamicConfig->AbortOnLocationDisabled) {
        TProgram::Abort(EProgramExitCode::ProgramError);
    }
}

void TChunkLocation::ResetLocationStatistic()
{
    VERIFY_INVOKER_AFFINITY(GetAuxPoolInvoker());

    AvailableSpace_.store(0);
    UsedSpace_.store(0);
    for (auto& count : PerTypeSessionCount_) {
        count.store(0);
    }
    ChunkCount_.store(0);

    ChunkStoreHost_->ScheduleMasterHeartbeat();
}

const TChunkStorePtr& TChunkLocation::GetChunkStore() const
{
    return ChunkStore_;
}

////////////////////////////////////////////////////////////////////////////////

class TStoreLocation::TIOStatisticsProvider
    : public NProfiling::ISensorProducer
{
public:
    TIOStatisticsProvider(
        TStoreLocationConfigPtr config,
        NIO::IIOEnginePtr ioEngine,
        TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
        NProfiling::TProfiler profiler,
        TLogger logger)
        : DeviceName_(config->DeviceName)
        , MaxWriteRateByDwpd_(config->MaxWriteRateByDwpd)
        , IOEngine_(std::move(ioEngine))
        , Logger(std::move(logger))
        , LastUpdateTime_(TInstant::Now())
        , LastCounters_(GetCounters())
    {
        dynamicConfigManager->SubscribeConfigChanged(
            BIND(&TIOStatisticsProvider::OnDynamicConfigChanged, MakeWeak(this)));

        profiler.AddProducer("", MakeStrong(this));
    }

    TIOStatistics Get()
    {
        auto guard = Guard(CountersLock_);

        if (TInstant::Now() > LastUpdateTime_ + UpdateStatisticsTimeout_) {
            Update();
        }

        return Statistics_;
    }

private:
    struct TCounters
    {
        i64 FilesystemRead = 0;
        i64 FilesystemWritten = 0;
        i64 DiskRead = 0;
        i64 DiskWritten = 0;
    };

    const TString DeviceName_;
    const i64 MaxWriteRateByDwpd_;
    const NIO::IIOEnginePtr IOEngine_;
    const TLogger Logger;
    std::atomic<TDuration> UpdateStatisticsTimeout_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, CountersLock_);
    TInstant LastUpdateTime_;
    std::optional<TCounters> LastCounters_;
    TIOStatistics Statistics_;

    bool ErrorLogged_ = false;

    std::optional<TCounters> GetCounters() const
    {
        try {
            auto counters = TCounters{
                .FilesystemRead = IOEngine_->GetTotalReadBytes(),
                .FilesystemWritten = IOEngine_->GetTotalWrittenBytes(),
            };

            auto diskStats = NYT::GetDiskStats();
            auto it = diskStats.find(DeviceName_);
            if (it != diskStats.end()) {
                counters.DiskRead = it->second.SectorsRead * UnixSectorSize;
                counters.DiskWritten = it->second.SectorsWritten * UnixSectorSize;
            } else {
                YT_LOG_WARNING("Cannot find disk IO statistics (DeviceName: %v)",
                    DeviceName_);
            }

            return counters;
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to get disk IO statistics");
        }
        return {};
    }

    static i64 CalculateRate(i64 oldValue, i64 newValue, TDuration duration)
    {
        auto seconds = static_cast<double>(duration.MilliSeconds()) / 1000;
        return static_cast<i64>(std::max<i64>(0, (newValue - oldValue)) / seconds);
    }

    void Update()
    {
        auto oldCounters = LastCounters_;
        auto currentCounters = GetCounters();
        auto now = TInstant::Now();
        auto duration = now - LastUpdateTime_;

        if (oldCounters && currentCounters) {
            Statistics_ = TIOStatistics{
                .FilesystemReadRate = CalculateRate(oldCounters->FilesystemRead, currentCounters->FilesystemRead, duration),
                .FilesystemWriteRate = CalculateRate(oldCounters->FilesystemWritten, currentCounters->FilesystemWritten, duration),
                .DiskReadRate = CalculateRate(oldCounters->DiskRead, currentCounters->DiskRead, duration),
                .DiskWriteRate = CalculateRate(oldCounters->DiskWritten, currentCounters->DiskWritten, duration),
            };
        }

        LastUpdateTime_ = now;
        LastCounters_ = currentCounters;
    }

     void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& /*oldConfig*/,
        const TClusterNodeDynamicConfigPtr& newConfig)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        UpdateStatisticsTimeout_.store(newConfig->DataNode->IOStatisticsUpdateTimeout);
    }

    void CollectSensors(ISensorWriter* writer) override
    {
        try {
            auto stats = GetDiskStats();
            auto it = stats.find(DeviceName_);
            if (it == stats.end()) {
                return;
            }
            const auto& diskStat = it->second;

            writer->AddCounter(
                "/disk/read_bytes",
                diskStat.SectorsRead * UnixSectorSize);

            writer->AddCounter(
                "/disk/written_bytes",
                diskStat.SectorsWritten * UnixSectorSize);

            writer->AddGauge(
                "/disk/io_in_progress",
                diskStat.IOCurrentlyInProgress);

            writer->AddGauge(
                "/disk/max_write_rate_by_dwpd",
                MaxWriteRateByDwpd_);

        } catch (const std::exception& ex) {
            if (!ErrorLogged_) {
                YT_LOG_ERROR(ex, "Failed to collect disk statistics");
                ErrorLogged_ = true;
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TStoreLocation::TStoreLocation(
    TString id,
    TStoreLocationConfigPtr config,
    TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
    TChunkStorePtr chunkStore,
    TChunkContextPtr chunkContext,
    IChunkStoreHostPtr chunkStoreHost)
    : TChunkLocation(
        ELocationType::Store,
        std::move(id),
        config,
        std::move(dynamicConfigManager),
        std::move(chunkStore),
        std::move(chunkContext),
        std::move(chunkStoreHost))
    , StaticConfig_(config)
    , JournalManager_(CreateJournalManager(
        BuildJournalManagerConfig(ChunkContext_->DataNodeConfig, config),
        this,
        ChunkContext_,
        ChunkStoreHost_->GetMemoryUsageTracker()))
    , TrashCheckQueue_(New<TActionQueue>(Format("Trash:%v", Id_)))
    , TrashCheckExecutor_(New<TPeriodicExecutor>(
        TrashCheckQueue_->GetInvoker(),
        BIND(&TStoreLocation::OnCheckTrash, MakeWeak(this)),
        config->TrashCheckPeriod))
    , IOStatisticsProvider_(New<TIOStatisticsProvider>(
        config,
        GetIOEngine(),
        DynamicConfigManager_,
        Profiler_,
        Logger))
    , RuntimeConfig_(config)
{ }

TStoreLocation::~TStoreLocation() = default;

const TStoreLocationConfigPtr& TStoreLocation::GetStaticConfig() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return StaticConfig_;
}

TStoreLocationConfigPtr TStoreLocation::GetRuntimeConfig() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return RuntimeConfig_.Acquire();
}

void TStoreLocation::Reconfigure(TStoreLocationConfigPtr config)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TChunkLocation::Reconfigure(config);

    JournalManager_->Reconfigure(BuildJournalManagerConfig(ChunkContext_->DataNodeConfig, config));

    TrashCheckExecutor_->SetPeriod(config->TrashCheckPeriod);

    RuntimeConfig_.Store(std::move(config));
}

const IJournalManagerPtr& TStoreLocation::GetJournalManager()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return JournalManager_;
}

i64 TStoreLocation::GetLowWatermarkSpace() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto config = GetRuntimeConfig();
    return config->LowWatermark;
}

i64 TStoreLocation::GetMaxWriteRateByDwpd() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto config = GetRuntimeConfig();
    return config->MaxWriteRateByDwpd;
}

bool TStoreLocation::IsFull() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto available = GetAvailableSpace();
    auto config = GetRuntimeConfig();
    auto watermark = Full_.load() ? config->LowWatermark : config->HighWatermark;
    auto full = available < watermark;
    auto expected = !full;
    if (Full_.compare_exchange_strong(expected, full)) {
        YT_LOG_DEBUG("Location is %v full (AvailableSpace: %v, WatermarkSpace: %v)",
            full ? "now" : "no longer",
            available,
            watermark);
    }
    return full;
}

bool TStoreLocation::HasEnoughSpace(i64 size) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto config = GetRuntimeConfig();
    return GetAvailableSpace() - size >= config->DisableWritesWatermark;
}

void TStoreLocation::RemoveChunkFiles(TChunkId chunkId, bool force)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto config = GetRuntimeConfig();

    if (force || config->MaxTrashTtl == TDuration::Zero()) {
        RemoveChunkFilesPermanently(chunkId);
    } else {
        MoveChunkFilesToTrash(chunkId);
    }
}

TJournalManagerConfigPtr TStoreLocation::BuildJournalManagerConfig(
    const TDataNodeConfigPtr& dataNodeConfig,
    const TStoreLocationConfigPtr& storeLocationConfig)
{
    auto journalManagerConfig = CloneYsonStruct(TJournalManagerConfigPtr(dataNodeConfig));
    journalManagerConfig->MultiplexedChangelog = UpdateYsonStruct(dataNodeConfig->MultiplexedChangelog, storeLocationConfig->MultiplexedChangelog);
    journalManagerConfig->HighLatencySplitChangelog = UpdateYsonStruct(dataNodeConfig->HighLatencySplitChangelog, storeLocationConfig->HighLatencySplitChangelog);
    journalManagerConfig->LowLatencySplitChangelog = UpdateYsonStruct(dataNodeConfig->LowLatencySplitChangelog, storeLocationConfig->LowLatencySplitChangelog);
    return journalManagerConfig;
}

TString TStoreLocation::GetTrashPath() const
{
    return NFS::CombinePaths(GetPath(), TrashDirectory);
}

TString TStoreLocation::GetTrashChunkPath(TChunkId chunkId) const
{
    return NFS::CombinePaths(GetTrashPath(), GetRelativeChunkPath(chunkId));
}

void TStoreLocation::RegisterTrashChunk(TChunkId chunkId)
{
    try {
        auto timestamp = TInstant::Zero();
        i64 diskSpace = 0;
        auto partNames = GetChunkPartNames(chunkId);
        for (const auto& name : partNames) {
            auto directory = NFS::GetDirectoryName(GetTrashChunkPath(chunkId));
            auto fileName = NFS::CombinePaths(directory, name);
            if (NFS::Exists(fileName)) {
                auto statistics = NFS::GetPathStatistics(fileName);
                timestamp = std::max(timestamp, statistics.ModificationTime);
                diskSpace += statistics.Size;
            }
        }

        {
            auto guard = Guard(TrashMapSpinLock_);
            TrashMap_.emplace(timestamp, TTrashChunkEntry{chunkId, diskSpace});
            TrashDiskSpace_ += diskSpace;
        }

        YT_LOG_DEBUG("Trash chunk registered (ChunkId: %v, Timestamp: %v, DiskSpace: %v)",
            chunkId,
            timestamp,
            diskSpace);
    } catch (const std::exception& ex) {
        // This is racy, trash file can be removed anytime.
        YT_LOG_WARNING(ex, "Failed to register trash chunk (ChunkId: %v)",
            chunkId);
    }
}

void TStoreLocation::OnCheckTrash()
{
    if (!IsEnabled())
        return;

    try {
        CheckTrashTtl();
        CheckTrashWatermark();
    } catch (const std::exception& ex) {
        auto error = TError("Error checking trash")
            << ex;
        ScheduleDisable(error);
    }
}

void TStoreLocation::CheckTrashTtl()
{
    auto config = GetRuntimeConfig();
    auto deadline = TInstant::Now() - config->MaxTrashTtl;
    while (true) {
        TTrashChunkEntry entry;
        {
            auto guard = Guard(TrashMapSpinLock_);
            if (TrashMap_.empty())
                break;
            auto it = TrashMap_.begin();
            if (it->first >= deadline)
                break;
            entry = it->second;
            TrashMap_.erase(it);
            TrashDiskSpace_ -= entry.DiskSpace;
        }
        RemoveTrashFiles(entry);
    }
}

void TStoreLocation::CheckTrashWatermark()
{
    auto config = GetRuntimeConfig();

    bool needsCleanup;
    i64 availableSpace;
    {
        auto guard = Guard(TrashMapSpinLock_);
        // NB: Available space includes trash disk space.
        availableSpace = GetAvailableSpace() - TrashDiskSpace_.load();
        needsCleanup = availableSpace < config->TrashCleanupWatermark && !TrashMap_.empty();
    }

    if (!needsCleanup) {
        return;
    }

    YT_LOG_INFO("Low available disk space, starting trash cleanup (AvailableSpace: %v)",
        availableSpace);

    while (availableSpace < config->TrashCleanupWatermark) {
        TTrashChunkEntry entry;
        {
            auto guard = Guard(TrashMapSpinLock_);
            if (TrashMap_.empty()) {
                break;
            }
            auto it = TrashMap_.begin();
            entry = it->second;
            TrashMap_.erase(it);
            TrashDiskSpace_ -= entry.DiskSpace;
        }
        RemoveTrashFiles(entry);
        availableSpace += entry.DiskSpace;
    }

    YT_LOG_INFO("Finished trash cleanup (AvailableSpace: %v)",
        availableSpace);
}

void TStoreLocation::RemoveTrashFiles(const TTrashChunkEntry& entry)
{
    auto partNames = GetChunkPartNames(entry.ChunkId);
    for (const auto& name : partNames) {
        auto directory = NFS::GetDirectoryName(GetTrashChunkPath(entry.ChunkId));
        auto fileName = NFS::CombinePaths(directory, name);
        if (NFS::Exists(fileName)) {
            NFS::Remove(fileName);
        }
    }

    YT_LOG_DEBUG("Trash chunk removed (ChunkId: %v, DiskSpace: %v)",
        entry.ChunkId,
        entry.DiskSpace);
}

void TStoreLocation::MoveChunkFilesToTrash(TChunkId chunkId)
{
    try {
        YT_LOG_DEBUG("Started moving chunk files to trash (ChunkId: %v)", chunkId);

        auto partNames = GetChunkPartNames(chunkId);
        auto directory = NFS::GetDirectoryName(GetChunkPath(chunkId));
        auto trashDirectory = NFS::GetDirectoryName(GetTrashChunkPath(chunkId));

        for (const auto& name : partNames) {
            auto srcFileName = NFS::CombinePaths(directory, name);
            auto dstFileName = NFS::CombinePaths(trashDirectory, name);
            if (NFS::Exists(srcFileName)) {
                NFS::Replace(srcFileName, dstFileName);
                NFS::Touch(dstFileName);
            }
        }

        YT_LOG_DEBUG("Finished moving chunk files to trash (ChunkId: %v)", chunkId);

        RegisterTrashChunk(chunkId);

        UnlockChunk(chunkId);
    } catch (const std::exception& ex) {
        auto error = TError(
            NChunkClient::EErrorCode::IOError,
            "Error moving chunk %v to trash",
            chunkId)
            << ex;
        ScheduleDisable(error);
    }
}

void TStoreLocation::RemoveLocationChunks()
{
    VERIFY_INVOKER_AFFINITY(GetAuxPoolInvoker());

    auto state = GetState();
    YT_LOG_FATAL_IF(
        state != ELocationState::Disabling,
        "Remove location chunks should be called when state is equal to ELocationState::Disabling");

    auto locationChunks = ChunkStore_->GetLocationChunks(MakeStrong(this));

    try {
        for (const auto& chunk : locationChunks) {
            ChunkStore_->UnregisterChunk(chunk);
            UnlockChunk(chunk->GetId());
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Cannot complete unregister chunk futures")
            << ex;
    }
}

bool TStoreLocation::ScheduleDisable(const TError& reason)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!ChangeState(ELocationState::Disabling, ELocationState::Enabled)) {
        return false;
    }

    YT_LOG_WARNING(reason, "Disabling location (LocationUuid: %v)", GetUuid());

    // No new actions can appear here. Please see TDiskLocation::RegisterAction.
    auto error = TError("Chunk location at %v is disabled", GetPath())
        << TErrorAttribute("location_uuid", GetUuid())
        << TErrorAttribute("location_path", GetPath())
        << TErrorAttribute("location_disk", StaticConfig_->DeviceName)
        << reason;
    LocationDisabledAlert_.Store(error);

    BIND([=, this, this_ = MakeStrong(this)] () {
        try {
            ChunkStoreHost_->CancelLocationSessions(MakeStrong(static_cast<TChunkLocation*>(this)));

            WaitFor(BIND(&TStoreLocation::SynchronizeActions, MakeStrong(this))
                .AsyncVia(GetAuxPoolInvoker())
                .Run())
                .ThrowOnError();

            CreateDisableLockFile(reason);
            RemoveLocationChunks();
            ResetLocationStatistic();
        } catch (const std::exception& ex) {
            YT_LOG_ALERT(ex, "Location disabling error");
        }

        auto finish = ChangeState(ELocationState::Disabled, ELocationState::Disabling);

        if (!finish) {
            YT_LOG_ALERT("Detect location state racing (CurrentState: %v)",
                GetState());
        }
    })
        .AsyncVia(GetAuxPoolInvoker())
        .Run();

    return true;
}

i64 TStoreLocation::GetAdditionalSpace() const
{
    // NB: Unguarded access to TrashDiskSpace_ seems OK.
    return TrashDiskSpace_.load();
}

std::optional<TChunkDescriptor> TStoreLocation::RepairBlobChunk(TChunkId chunkId)
{
    auto fileName = GetChunkPath(chunkId);
    auto trashFileName = GetTrashChunkPath(chunkId);

    auto dataFileName = fileName;
    auto metaFileName = fileName + ChunkMetaSuffix;

    auto trashDataFileName = trashFileName;
    auto trashMetaFileName = trashFileName + ChunkMetaSuffix;

    bool hasData = NFS::Exists(dataFileName);
    bool hasMeta = NFS::Exists(metaFileName);

    if (hasMeta && hasData) {
        i64 dataSize = NFS::GetPathStatistics(dataFileName).Size;
        i64 metaSize = NFS::GetPathStatistics(metaFileName).Size;
        if (metaSize > 0) {
            TChunkDescriptor descriptor;
            descriptor.Id = chunkId;
            descriptor.DiskSpace = dataSize + metaSize;
            return descriptor;
        }
        // EXT4 specific thing.
        // See https://bugs.launchpad.net/ubuntu/+source/linux/+bug/317781
        YT_LOG_WARNING("Chunk meta file %v is empty, removing chunk files",
            metaFileName);
        NFS::Remove(dataFileName);
        NFS::Remove(metaFileName);
    } else if (!hasMeta && hasData) {
        YT_LOG_WARNING("Chunk meta file %v is missing, moving data file %v to trash",
            metaFileName,
            dataFileName);
        NFS::Replace(dataFileName, trashDataFileName);
    } else if (!hasData && hasMeta) {
        YT_LOG_WARNING("Chunk data file %v is missing, moving meta file %v to trash",
            dataFileName,
            metaFileName);
        NFS::Replace(metaFileName, trashMetaFileName);
    }
    return {};
}

std::optional<TChunkDescriptor> TStoreLocation::RepairJournalChunk(TChunkId chunkId)
{
    auto fileName = GetChunkPath(chunkId);
    auto trashFileName = GetTrashChunkPath(chunkId);

    auto dataFileName = fileName;
    auto indexFileName = fileName + "." + ChangelogIndexExtension;

    auto trashIndexFileName = trashFileName + "." + ChangelogIndexExtension;

    bool hasData = NFS::Exists(dataFileName);
    bool hasIndex = NFS::Exists(indexFileName);

    if (hasData) {
        const auto& dispatcher = ChunkContext_->JournalDispatcher;
        // NB: This also creates the index file, if missing.
        auto changelog = WaitFor(dispatcher->OpenJournal(this, chunkId))
            .ValueOrThrow();
        TChunkDescriptor descriptor;
        descriptor.Id = chunkId;
        descriptor.DiskSpace = changelog->GetDataSize();
        descriptor.RowCount = changelog->GetRecordCount();
        descriptor.Sealed = WaitFor(dispatcher->IsJournalSealed(this, chunkId))
            .ValueOrThrow();
        return descriptor;

    } else if (!hasData && hasIndex) {
        YT_LOG_WARNING("Journal data file %v is missing, moving index file %v to trash",
            dataFileName,
            indexFileName);
        NFS::Replace(indexFileName, trashIndexFileName);
    }

    return {};
}

std::optional<TChunkDescriptor> TStoreLocation::RepairChunk(TChunkId chunkId)
{
    std::optional<TChunkDescriptor> optionalDescriptor;
    auto chunkType = TypeFromId(DecodeChunkId(chunkId).Id);
    switch (chunkType) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
            optionalDescriptor = RepairBlobChunk(chunkId);
            break;

        case EObjectType::JournalChunk:
        case EObjectType::ErasureJournalChunk:
            optionalDescriptor = RepairJournalChunk(chunkId);
            break;

        default:
            YT_LOG_WARNING("Invalid chunk type, skipped (ChunkId: %v, ChunkType: %v)",
                chunkId,
                chunkType);
            break;
    }
    return optionalDescriptor;
}

std::vector<TString> TStoreLocation::GetChunkPartNames(TChunkId chunkId) const
{
    auto primaryName = ToString(chunkId);
    switch (TypeFromId(DecodeChunkId(chunkId).Id)) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
            return {
                primaryName,
                primaryName + ChunkMetaSuffix
            };

        case EObjectType::JournalChunk:
        case EObjectType::ErasureJournalChunk:
            return {
                primaryName,
                primaryName + "." + ChangelogIndexExtension,
                primaryName + "." + SealedFlagExtension
            };

        default:
            YT_ABORT();
    }
}

bool TStoreLocation::ShouldSkipFileName(const TString& fileName) const
{
    if (TChunkLocation::ShouldSkipFileName(fileName)) {
        return true;
    }

    // Skip trash directory.
    if (fileName.StartsWith(TrashDirectory + LOCSLASH_S))
        return true;

    // Skip multiplexed directory.
    if (fileName.StartsWith(MultiplexedDirectory + LOCSLASH_S))
        return true;

    return false;
}

std::vector<TChunkDescriptor> TStoreLocation::DoScan()
{
    auto result = TChunkLocation::DoScan();

    YT_LOG_INFO("Started scanning location trash");

    ForceHashDirectories(GetTrashPath());

    THashSet<TChunkId> trashChunkIds;
    {
        // Enumerate files under the location's trash directory.
        // Note that some of them might have just been moved there during repair.
        auto fileNames = NFS::EnumerateFiles(GetTrashPath(), std::numeric_limits<int>::max());

        for (const auto& fileName : fileNames) {
            TChunkId chunkId;
            auto bareFileName = NFS::GetFileNameWithoutExtension(fileName);
            if (!TChunkId::FromString(bareFileName, &chunkId)) {
                YT_LOG_ERROR("Unrecognized file in location trash directory (FileName: %v)", fileName);
                continue;
            }
            trashChunkIds.insert(chunkId);
        }

        for (auto chunkId : trashChunkIds) {
            RegisterTrashChunk(chunkId);
        }
    }

    YT_LOG_INFO("Finished scanning location trash (ChunkCount: %v)",
        trashChunkIds.size());

    return result;
}

void TStoreLocation::DoStart()
{
    TChunkLocation::DoStart();

    JournalManager_->Initialize();

    TrashCheckExecutor_->Start();
}

TStoreLocation::TIOStatistics TStoreLocation::GetIOStatistics() const
{
    return IOStatisticsProvider_->Get();
}

bool TStoreLocation::IsWritable() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!IsEnabled()) {
        return false;
    }

    if (IsFull()) {
        return false;
    }

    if (IsSick()) {
        return false;
    }

    if (GetMaxPendingIOSize(EIODirection::Write) > GetWriteThrottlingLimit()) {
        return false;
    }

    auto dynamicConfig = DynamicConfigManager_->GetConfig()->DataNode;
    if (dynamicConfig->DisableLocationWritesPendingReadSizeLowLimit && dynamicConfig->DisableLocationWritesPendingReadSizeHighLimit) {
        auto pendingReadSize = GetMaxPendingIOSize(EIODirection::Read);
        if (WritesDisabledDueToHighPendingReadSize_.load()) {
            if (pendingReadSize < *dynamicConfig->DisableLocationWritesPendingReadSizeLowLimit) {
                WritesDisabledDueToHighPendingReadSize_.store(false);
            }
        } else {
            if (pendingReadSize > *dynamicConfig->DisableLocationWritesPendingReadSizeHighLimit) {
                WritesDisabledDueToHighPendingReadSize_.store(true);
                return false;
            }
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
