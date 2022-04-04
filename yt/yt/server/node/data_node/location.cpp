#include "location.h"

#include "private.h"
#include "blob_chunk.h"
#include "blob_reader_cache.h"
#include "chunk_store.h"
#include "config.h"
#include "journal_chunk.h"
#include "journal_dispatcher.h"
#include "journal_manager.h"
#include "legacy_master_connector.h"
#include "master_connector.h"
#include "medium_updater.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/lib/hydra_common/changelog.h>
#include <yt/yt/server/lib/hydra_common/private.h>

#include <yt/yt/server/lib/misc/disk_health_checker.h>
#include <yt/yt/server/lib/misc/private.h>

#include <yt/yt/server/lib/io/io_engine.h>
#include <yt/yt/server/lib/io/io_workload_model.h>

#include <yt/yt/ytlib/chunk_client/format.h>
#include <yt/yt/ytlib/chunk_client/medium_directory_synchronizer.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/program/program.h>

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
                TDuration::MilliSeconds(1),
                TDuration::Seconds(1));
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

void TLocationPerformanceCounters::ThrottleRead()
{
    ThrottledReads.Increment();
    LastReadThrottleTime = GetCpuInstant();
}

void TLocationPerformanceCounters::ThrottleWrite()
{
    ThrottledWrites.Increment();
    LastWriteThrottleTime = GetCpuInstant();
}

////////////////////////////////////////////////////////////////////////////////

TChunkLocation::TChunkLocation(
    ELocationType type,
    const TString& id,
    TStoreLocationConfigBasePtr config,
    NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
    TChunkStorePtr chunkStore,
    TChunkContextPtr chunkContext,
    IChunkStoreHostPtr chunkStoreHost)
    : TDiskLocation(config, id, DataNodeLogger)
    , DynamicConfigManager_(dynamicConfigManager)
    , ChunkStore_(chunkStore)
    , ChunkContext_(chunkContext)
    , ChunkStoreHost_(chunkStoreHost)
    , Type_(type)
    , Config_(config)
    , MediumDescriptor_(TMediumDescriptor{.Name = Config_->MediumName})
{
    Profiler_ = LocationProfiler
        .WithSparse()
        .WithTag("location_type", FormatEnum(Type_))
        .WithTag("medium", GetMediumName(), -1)
        .WithTag("location_id", Id_, -1)
        .WithExtensionTag("device_name", Config_->DeviceName, -1)
        .WithExtensionTag("device_model", Config_->DeviceModel, -1);

    PerformanceCounters_ = New<TLocationPerformanceCounters>(Profiler_);

    auto engine = CreateIOEngine(
        Config_->IOEngineType,
        Config_->IOConfig,
        id,
        Profiler_,
        DataNodeLogger.WithTag("LocationId: %v", id));

    IOEngineModel_ = CreateIOModelInterceptor(id, engine, DataNodeLogger.WithTag("IOModel: %v", id));
    IOEngine_ = IOEngineModel_;

    auto createThrottler = [&] (const auto& config, const auto& name) {
        return CreateNamedReconfigurableThroughputThrottler(config, name, Logger, Profiler_);
    };

    ReplicationOutThrottler_ = createThrottler(config->ReplicationOutThrottler, "ReplicationOutThrottler");
    TabletCompactionAndPartitioningOutThrottler_ = createThrottler(
        config->TabletCompactionAndPartitioningOutThrottler,
        "TabletCompactionAndPartitioningOutThrottler");
    TabletLoggingOutThrottler_ = createThrottler(config->TabletLoggingOutThrottler, "TabletLoggingOutThrottler");
    TabletPreloadOutThrottler_ = createThrottler(config->TabletPreloadOutThrottler, "TabletPreloadOutThrottler");
    TabletRecoveryOutThrottler_ = createThrottler(config->TabletRecoveryOutThrottler, "TabletRecoveryOutThrottler");
    UnlimitedOutThrottler_ = CreateNamedUnlimitedThroughputThrottler("UnlimitedOutThrottler", Profiler_);

    HealthChecker_ = New<TDiskHealthChecker>(
        ChunkContext_->DataNodeConfig->DiskHealthChecker,
        GetPath(),
        GetAuxPoolInvoker(),
        DataNodeLogger,
        Profiler_);

    ChunkStoreHost_->SubscribePopulateAlerts(BIND(&TChunkLocation::PopulateAlerts, MakeWeak(this)));
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

    return Config_->DiskFamily;
}

NIO::EDirectIOPolicy TChunkLocation::UseDirectIOForReads() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Config_->UseDirectIOForReads;
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

    return Config_->Path;
}

i64 TChunkLocation::GetQuota() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Config_->Quota.value_or(std::numeric_limits<i64>::max());
}

i64 TChunkLocation::GetCoalescedReadMaxGapSize() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Config_->CoalescedReadMaxGapSize;
}

const IInvokerPtr& TChunkLocation::GetAuxPoolInvoker()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return IOEngine_->GetAuxPoolInvoker();
}

std::vector<TChunkDescriptor> TChunkLocation::Scan()
{
    try {
        ValidateLockFile();
        ValidateMinimumSpace();
        ValidateWritable();
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Location disabled");
        MarkAsDisabled(ex);
        return {};
    }

    // Be optimistic and assume everything will be OK.
    // Also Disable requires Enabled_ to be true.
    Enabled_.store(true);

    try {
        return DoScan();
    } catch (const std::exception& ex) {
        Disable(TError("Location scan failed") << ex);
        return {};
    }
}

void TChunkLocation::Start()
{
    if (!IsEnabled()) {
        return;
    }

    try {
        DoStart();
    } catch (const std::exception& ex) {
        Disable(TError("Location start failed") << ex);
    }
}

void TChunkLocation::Disable(const TError& reason)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!Enabled_.exchange(false)) {
        YT_LOG_ERROR(reason, "Attempted to disable location, but it is already disabled");
        return;
    }

    YT_LOG_ERROR(reason, "Disabling location");

    // Save the reason in a file and exit.
    // Location will be disabled during the scan in the restart process.
    auto lockFilePath = NFS::CombinePaths(GetPath(), DisabledLockFileName);
    try {
        TFile file(lockFilePath, CreateAlways | WrOnly | Seq | CloseOnExec);
        TUnbufferedFileOutput fileOutput(file);
        fileOutput << ConvertToYsonString(reason, NYson::EYsonFormat::Pretty).AsStringBuf();
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Error creating location lock file; aborting");
        TProgram::Abort(EProgramExitCode::ProgramError);
    }

    const auto& dynamicConfig = DynamicConfigManager_->GetConfig()->DataNode;
    if (dynamicConfig->AbortOnLocationDisabled) {
        TProgram::Abort(EProgramExitCode::ProgramError);
    }

    Disabled_.Fire();

    ChunkStoreHost_->ScheduleMasterHeartbeat();
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
        const_cast<TChunkLocation*>(this)->Disable(error);
    }

    i64 remainingQuota = std::max(static_cast<i64>(0), GetQuota() - GetUsedSpace());
    availableSpace = std::min(availableSpace, remainingQuota);
    AvailableSpace_.store(availableSpace);

    return availableSpace;
}

i64 TChunkLocation::GetPendingIOSize(
    EIODirection direction,
    const TWorkloadDescriptor& workloadDescriptor)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto category = ToIOCategory(workloadDescriptor);
    return PerformanceCounters_->PendingIOSize[direction][category].load();
}

i64 TChunkLocation::GetMaxPendingIOSize(EIODirection direction)
{
    VERIFY_THREAD_AFFINITY_ANY();

    i64 result = 0;
    for (auto category : TEnumTraits<EIOCategory>::GetDomainValues()) {
        result = std::max(result, PerformanceCounters_->PendingIOSize[direction][category].load());
    }
    return result;
}

TPendingIOGuard TChunkLocation::IncreasePendingIOSize(
    EIODirection direction,
    const TWorkloadDescriptor& workloadDescriptor,
    i64 delta)
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_ASSERT(delta >= 0);
    auto category = ToIOCategory(workloadDescriptor);
    UpdatePendingIOSize(direction, category, delta);
    return TPendingIOGuard(direction, category, delta, this);
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

        Unlock(chunkId);

        YT_LOG_DEBUG("Finished removing chunk files (ChunkId: %v)", chunkId);
    } catch (const std::exception& ex) {
        auto error = TError(
            NChunkClient::EErrorCode::IOError,
            "Error removing chunk %v",
            chunkId)
            << ex;
        Disable(error);
    }
}

void TChunkLocation::RemoveChunkFiles(TChunkId chunkId, bool /*force*/)
{
    VERIFY_THREAD_AFFINITY_ANY();

    RemoveChunkFilesPermanently(chunkId);
}

IThroughputThrottlerPtr TChunkLocation::GetOutThrottler(const TWorkloadDescriptor& descriptor) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    switch (descriptor.Category) {
        case EWorkloadCategory::SystemReplication:
            return ReplicationOutThrottler_;

        case EWorkloadCategory::SystemTabletCompaction:
        case EWorkloadCategory::SystemTabletPartitioning:
            return TabletCompactionAndPartitioningOutThrottler_;

        case EWorkloadCategory::SystemTabletLogging:
             return TabletLoggingOutThrottler_;

        case EWorkloadCategory::SystemTabletPreload:
             return TabletPreloadOutThrottler_;

        case EWorkloadCategory::SystemTabletRecovery:
            return TabletRecoveryOutThrottler_;

        default:
            return UnlimitedOutThrottler_;
    }
}

bool TChunkLocation::IsReadThrottling()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto time = PerformanceCounters_->LastReadThrottleTime.load();
    return GetCpuInstant() < time + 2 * DurationToCpuDuration(Config_->ThrottleDuration);
}

bool TChunkLocation::IsWriteThrottling()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto time = PerformanceCounters_->LastWriteThrottleTime.load();
    return GetCpuInstant() < time + 2 * DurationToCpuDuration(Config_->ThrottleDuration);
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

bool TChunkLocation::IsSick() const
{
    return IOEngine_->IsSick();
}

bool TChunkLocation::TryLock(TChunkId chunkId, bool verbose)
{
    auto guard = Guard(LockedChunksLock_);
    if (LockedChunks_.emplace(chunkId).second) {
        if (verbose) {
            YT_LOG_DEBUG("Locked chunk (ChunkId: %v)",
                chunkId);
        }
        return true;
    } else {
        return false;
    }
}

void TChunkLocation::Unlock(TChunkId chunkId)
{
    auto guard = Guard(LockedChunksLock_);
    if (LockedChunks_.erase(chunkId)) {
        YT_LOG_DEBUG("Unlocked chunk (ChunkId: %v)",
            chunkId);
    } else {
        YT_LOG_WARNING("Attempted to unlock non-locked chunk (ChunkId: %v)",
            chunkId);
    }
}

void TChunkLocation::OnHealthCheckFailed(const TError& error)
{
    Disable(error);
}

void TChunkLocation::MarkAsDisabled(const TError& error)
{
    LocationDisabledAlert_.Store(TError("Chunk location at %v is disabled", GetPath())
        << error);

    Enabled_.store(false);
    AvailableSpace_.store(0);
    UsedSpace_.store(0);
    for (auto& count : PerTypeSessionCount_) {
        count.store(0);
    }
    ChunkCount_.store(0);

    Profiler_
        .WithTag("error_code", ToString(static_cast<int>(error.GetNonTrivialCode())))
        .AddFuncGauge("/disabled", MakeStrong(this), [] { return 1.0; });
}

i64 TChunkLocation::GetAdditionalSpace() const
{
    return 0;
}

bool TChunkLocation::ShouldSkipFileName(const TString& fileName) const
{
    return
        fileName == CellIdFileName ||
        fileName == ChunkLocationUuidFileName;
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
            TryLock(chunkId, /* verbose */ false);
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
    InitializeCellId();
    InitializeUuid();

    HealthChecker_->SubscribeFailed(BIND(&TChunkLocation::OnHealthCheckFailed, Unretained(this)));
    HealthChecker_->Start();
}

void TChunkLocation::UpdateMediumDescriptor(const NChunkClient::TMediumDescriptor& newDescriptor, bool onInitialize)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YT_VERIFY(newDescriptor.Index != GenericMediumIndex);

    auto oldDescriptor = MediumDescriptor_.Exchange(newDescriptor);

    MediumAlert_.Store(TError());

    if (newDescriptor == oldDescriptor) {
        return;
    }

    if (ChunkStore_ && !onInitialize && newDescriptor.Index != oldDescriptor.Index) {
        ChunkStore_->ChangeLocationMedium(this, oldDescriptor.Index);
    }

    YT_LOG_INFO("Location medium descriptor %v (Location: %v, MediumName: %v, MediumIndex: %v, Priority: %v)",
        onInitialize ? "set" : "changed",
        GetId(),
        newDescriptor.Name,
        newDescriptor.Index,
        newDescriptor.Priority);
}

bool TChunkLocation::UpdateMediumName(
    const TString& newMediumName,
    const TMediumDirectoryPtr& mediumDirectory,
    bool onInitialize)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    const auto* newDescriptor = mediumDirectory->FindByName(newMediumName);

    if (!newDescriptor) {
        auto error = TError("Location %Qv refers to unknown medium %Qv",
            GetId(),
            newMediumName);
        YT_LOG_WARNING(error);
        MediumAlert_.Store(std::move(error));
        return false;
    }

    UpdateMediumDescriptor(*newDescriptor, onInitialize);
    return true;
}

void TChunkLocation::PopulateAlerts(std::vector<TError>* alerts)
{
    for (const auto* alertHolder : {&LocationDisabledAlert_, &MediumAlert_}) {
        if (auto alert = alertHolder->Load(); !alert.IsOK()) {
            alerts->push_back(std::move(alert));
        }
    }
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
        NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
        NProfiling::TProfiler profiler,
        TLogger logger)
        : DeviceName_(config->DeviceName)
        , MaxWriteRateByDWPD_(config->MaxWriteRateByDWPD)
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
    const i64 MaxWriteRateByDWPD_;
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
                MaxWriteRateByDWPD_);

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
    const TString& id,
    TStoreLocationConfigPtr config,
    NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
    TChunkStorePtr chunkStore,
    TChunkContextPtr chunkContext,
    IChunkStoreHostPtr chunkStoreHost)
    : TChunkLocation(
        ELocationType::Store,
        id,
        config,
        dynamicConfigManager,
        chunkStore,
        chunkContext,
        chunkStoreHost)
    , Config_(config)
    , JournalManager_(New<TJournalManager>(
        chunkContext->DataNodeConfig,
        this,
        chunkContext))
    , TrashCheckQueue_(New<TActionQueue>(Format("Trash:%v", id)))
    , TrashCheckExecutor_(New<TPeriodicExecutor>(
        TrashCheckQueue_->GetInvoker(),
        BIND(&TStoreLocation::OnCheckTrash, MakeWeak(this)),
        Config_->TrashCheckPeriod))
    , StatisticsProvider_(New<TIOStatisticsProvider>(
        Config_,
        GetIOEngine(),
        dynamicConfigManager,
        Profiler_,
        Logger))
{
    YT_VERIFY(chunkStore);

    auto diskThrottlerProfiler = GetProfiler().WithPrefix("/disk_throttler");
    auto createThrottler = [&] (const auto& config, const auto& name) {
        return CreateNamedReconfigurableThroughputThrottler(config, name, Logger, diskThrottlerProfiler);
    };

    RepairInThrottler_ = createThrottler(config->RepairInThrottler, "RepairIn");
    ReplicationInThrottler_ = createThrottler(config->ReplicationInThrottler, "ReplicationIn");
    TabletCompactionAndPartitioningInThrottler_ = createThrottler(
        config->TabletCompactionAndPartitioningInThrottler,
        "TabletCompactionAndPartitioningIn");
    TabletLoggingInThrottler_ = createThrottler(config->TabletLoggingInThrottler, "TabletLoggingIn");
    TabletSnapshotInThrottler_ = createThrottler(config->TabletSnapshotInThrottler, "TabletSnapshotIn");
    TabletStoreFlushInThrottler_ = createThrottler(config->TabletStoreFlushInThrottler, "TabletStoreFlushIn");
    UnlimitedInThrottler_ = CreateNamedUnlimitedThroughputThrottler("UnlimitedIn", diskThrottlerProfiler);
}

TStoreLocation::~TStoreLocation()
{ }

const TStoreLocationConfigPtr& TStoreLocation::GetConfig() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Config_;
}

const TJournalManagerPtr& TStoreLocation::GetJournalManager()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return JournalManager_;
}

i64 TStoreLocation::GetLowWatermarkSpace() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Config_->LowWatermark;
}

i64 TStoreLocation::GetMaxWriteRateByDWPD() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Config_->MaxWriteRateByDWPD;
}

bool TStoreLocation::IsFull() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto available = GetAvailableSpace();
    auto watermark = Full_.load() ? Config_->LowWatermark : Config_->HighWatermark;
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

    return GetAvailableSpace() - size >= Config_->DisableWritesWatermark;
}

const IThroughputThrottlerPtr& TStoreLocation::GetInThrottler(const TWorkloadDescriptor& descriptor) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    switch (descriptor.Category) {
        case EWorkloadCategory::SystemRepair:
            return RepairInThrottler_;

        case EWorkloadCategory::SystemReplication:
            return ReplicationInThrottler_;

        case EWorkloadCategory::SystemTabletLogging:
            return TabletLoggingInThrottler_;

        case EWorkloadCategory::SystemTabletCompaction:
        case EWorkloadCategory::SystemTabletPartitioning:
            return TabletCompactionAndPartitioningInThrottler_;

        case EWorkloadCategory::SystemTabletSnapshot:
            return TabletSnapshotInThrottler_;

        case EWorkloadCategory::SystemTabletStoreFlush:
            return TabletStoreFlushInThrottler_;

        default:
            return UnlimitedInThrottler_;
    }
}

void TStoreLocation::RemoveChunkFiles(TChunkId chunkId, bool force)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (force || Config_->MaxTrashTtl == TDuration::Zero()) {
        RemoveChunkFilesPermanently(chunkId);
    } else {
        MoveChunkFilesToTrash(chunkId);
    }
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
                auto statistics = NFS::GetFileStatistics(fileName);
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
        Disable(error);
    }
}

void TStoreLocation::CheckTrashTtl()
{
    auto deadline = TInstant::Now() - Config_->MaxTrashTtl;
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
    bool needsCleanup;
    i64 availableSpace;
    {
        auto guard = Guard(TrashMapSpinLock_);
        // NB: Available space includes trash disk space.
        availableSpace = GetAvailableSpace() - TrashDiskSpace_.load();
        needsCleanup = availableSpace < Config_->TrashCleanupWatermark && !TrashMap_.empty();
    }

    if (!needsCleanup) {
        return;
    }

    YT_LOG_INFO("Low available disk space, starting trash cleanup (AvailableSpace: %v)",
        availableSpace);

    while (availableSpace < Config_->TrashCleanupWatermark) {
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

        Unlock(chunkId);
    } catch (const std::exception& ex) {
        auto error = TError(
            NChunkClient::EErrorCode::IOError,
            "Error moving chunk %v to trash",
            chunkId)
            << ex;
        Disable(error);
    }
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
        i64 dataSize = NFS::GetFileStatistics(dataFileName).Size;
        i64 metaSize = NFS::GetFileStatistics(metaFileName).Size;
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
        auto changelog = WaitFor(dispatcher->OpenChangelog(this, chunkId))
            .ValueOrThrow();
        TChunkDescriptor descriptor;
        descriptor.Id = chunkId;
        descriptor.DiskSpace = changelog->GetDataSize();
        descriptor.RowCount = changelog->GetRecordCount();
        descriptor.Sealed = WaitFor(dispatcher->IsChangelogSealed(this, chunkId))
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


TStoreLocation::TIOStatistics TStoreLocation::GetIOStatistics()
{
    return StatisticsProvider_->Get();
}

////////////////////////////////////////////////////////////////////////////////

TCacheLocation::TCacheLocation(
    const TString& id,
    TCacheLocationConfigPtr config,
    NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
    TChunkContextPtr chunkContext,
    IChunkStoreHostPtr chunkStoreHost)
    : TChunkLocation(
        ELocationType::Cache,
        id,
        config,
        dynamicConfigManager,
        nullptr,
        chunkContext,
        chunkStoreHost)
    , Config_(config)
    , InThrottler_(CreateNamedReconfigurableThroughputThrottler(
        Config_->InThrottler,
        "InThrottler",
        Logger,
        Profiler_.WithPrefix("/cache")))
{ }

const IThroughputThrottlerPtr& TCacheLocation::GetInThrottler() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return InThrottler_;
}

std::optional<TChunkDescriptor> TCacheLocation::Repair(
    TChunkId chunkId,
    const TString& metaSuffix)
{
    auto fileName = GetChunkPath(chunkId);

    auto dataFileName = fileName;
    auto metaFileName = fileName + metaSuffix;

    bool hasData = NFS::Exists(dataFileName);
    bool hasMeta = NFS::Exists(metaFileName);

    if (hasMeta && hasData) {
        i64 dataSize = NFS::GetFileStatistics(dataFileName).Size;
        i64 metaSize = NFS::GetFileStatistics(metaFileName).Size;
        if (metaSize > 0) {
            TChunkDescriptor descriptor;
            descriptor.Id = chunkId;
            descriptor.DiskSpace = dataSize + metaSize;
            return descriptor;
        }
        YT_LOG_WARNING("Chunk meta file %v is empty, removing chunk files",
            metaFileName);
    } else if (hasData && !hasMeta) {
        YT_LOG_WARNING("Chunk meta file %v is missing, removing data file %v",
            metaFileName,
            dataFileName);
    } else if (!hasData && hasMeta) {
        YT_LOG_WARNING("Chunk data file %v is missing, removing meta file %v",
            dataFileName,
            metaFileName);
    }

    if (hasData) {
        NFS::Remove(dataFileName);
    }
    if (hasMeta) {
        NFS::Remove(metaFileName);
    }

    return {};
}

std::optional<TChunkDescriptor> TCacheLocation::RepairChunk(TChunkId chunkId)
{
    std::optional<TChunkDescriptor> optionalDescriptor;
    auto chunkType = TypeFromId(DecodeChunkId(chunkId).Id);
    switch (chunkType) {
        case EObjectType::Chunk:
            optionalDescriptor = Repair(chunkId, ChunkMetaSuffix);
            break;

        case EObjectType::Artifact:
            optionalDescriptor = Repair(chunkId, ArtifactMetaSuffix);
            break;

        default:
            YT_LOG_WARNING("Invalid type %Qlv of chunk %v, skipped",
                chunkType,
                chunkId);
            break;
    }
    return optionalDescriptor;
}

std::vector<TString> TCacheLocation::GetChunkPartNames(TChunkId chunkId) const
{
    auto primaryName = ToString(chunkId);
    switch (TypeFromId(DecodeChunkId(chunkId).Id)) {
        case EObjectType::Chunk:
            return {
                primaryName,
                primaryName + ChunkMetaSuffix
            };

        case EObjectType::Artifact:
            return {
                primaryName,
                primaryName + ArtifactMetaSuffix
            };

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

TPendingIOGuard::TPendingIOGuard(
    EIODirection direction,
    EIOCategory category,
    i64 size,
    TChunkLocationPtr owner)
    : Direction_(direction)
    , Category_(category)
    , Size_(size)
    , Owner_(owner)
{ }

TPendingIOGuard::~TPendingIOGuard()
{
    Release();
}

void TPendingIOGuard::Release()
{
    if (Owner_) {
        Owner_->DecreasePendingIOSize(Direction_, Category_, Size_);
        Owner_.Reset();
    }
}

TPendingIOGuard::operator bool() const
{
    return Owner_.operator bool();
}

i64 TPendingIOGuard::GetSize() const
{
    return Size_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
