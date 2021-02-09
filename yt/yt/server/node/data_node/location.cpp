#include "location.h"
#include "private.h"
#include "blob_chunk.h"
#include "blob_reader_cache.h"
#include "config.h"
#include "journal_chunk.h"
#include "journal_dispatcher.h"
#include "journal_manager.h"
#include "master_connector.h"

#include <yt/server/node/cluster_node/bootstrap.h>
#include <yt/server/node/cluster_node/config.h>

#include <yt/server/lib/hydra/changelog.h>
#include <yt/server/lib/hydra/private.h>

#include <yt/server/lib/misc/disk_health_checker.h>
#include <yt/server/lib/misc/private.h>

#include <yt/ytlib/chunk_client/format.h>
#include <yt/ytlib/chunk_client/io_engine.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/misc/fs.h>

#include <yt/core/logging/log_manager.h>

#include <yt/core/concurrency/thread_pool.h>

namespace NYT::NDataNode {

using namespace NChunkClient;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NHydra;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

// Others must not be able to list chunk store and chunk cache directories.
static const int ChunkFilesPermissions = 0751;

////////////////////////////////////////////////////////////////////////////////

TLocationPerformanceCounters::TLocationPerformanceCounters(const NProfiling::TRegistry& registry)
{
    for (auto direction : TEnumTraits<EIODirection>::GetDomainValues()) {
        for (auto category : TEnumTraits<EIOCategory>::GetDomainValues()) {
            // Do not export both location_id and (direction, category).
            auto r = registry
                .WithAlternativeTag("direction", FormatEnum(direction), -1)
                .WithAlternativeTag("category", FormatEnum(category), -2);

            r.AddFuncGauge("/pending_data_size", MakeStrong(this), [this, direction, category] {
                return PendingIOSize[direction][category].load();
            });

            CompletedIOSize[direction][category] = r.Counter("/blob_block_bytes");
        }
    }

    ThrottledReads = registry.Counter("/throttled_reads");
    ThrottledWrites = registry.Counter("/throttled_writes");

    PutBlocksWallTime = registry.Timer("/put_blocks_wall_time");
    BlobChunkMetaReadTime = registry.Timer("/blob_chunk_meta_read_time");

    BlobChunkReaderOpenTime = registry.Timer("/blob_chunk_reader_open_time");

    BlobChunkWriterOpenTime = registry.Timer("/blob_chunk_writer_open_time");
    BlobChunkWriterAbortTime = registry.Timer("/blob_chunk_writer_abort_time");
    BlobChunkWriterCloseTime = registry.Timer("/blob_chunk_writer_close_time");

    BlobBlockReadSize = registry.Summary("/blob_block_read_size");
    BlobBlockReadTime = registry.Timer("/blob_block_read_time");
    BlobBlockReadBytes = registry.Counter("/blob_block_read_bytes");

    for (auto category : TEnumTraits<EWorkloadCategory>::GetDomainValues()) {
        // Do not export both location_id and category.
        auto r = registry.WithAlternativeTag("category", FormatEnum(category), -1);

        BlobBlockReadLatencies[category] = r.Timer("/blob_block_read_latency");
        BlobChunkMetaReadLatencies[category] = r.Timer("/blob_chunk_meta_read_latency");
    }

    BlobBlockWriteSize = registry.Summary("/blob_block_write_size");
    BlobBlockWriteTime = registry.Timer("/blob_block_write_time");
    BlobBlockWriteBytes = registry.Counter("/blob_block_write_bytes");

    JournalBlockReadSize = registry.Summary("/journal_block_read_size");
    JournalBlockReadTime = registry.Timer("/journal_block_read_time");
    JournalBlockReadBytes = registry.Counter("/journal_block_read_bytes");

    JournalChunkCreateTime = registry.Timer("/journal_chunk_create_time");
    JournalChunkOpenTime = registry.Timer("/journal_chunk_open_time");
    JournalChunkRemoveTime = registry.Timer("/journal_chunk_remove_time");

    for (auto type : TEnumTraits<ESessionType>::GetDomainValues()) {
        registry.WithTag("type", FormatEnum(type)).AddFuncGauge("/session_count", MakeStrong(this), [this, type] {
            return SessionCount[type].load();
        });
    }

    UsedSpace = registry.Gauge("/used_space");
    AvailableSpace = registry.Gauge("/available_space");
    Full = registry.Gauge("/full");
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

TLocation::TLocation(
    ELocationType type,
    const TString& id,
    TStoreLocationConfigBasePtr config,
    TBootstrap* bootstrap)
    : TDiskLocation(config, id, DataNodeLogger)
    , Bootstrap_(bootstrap)
    , Type_(type)
    , Config_(config)
{
    Profiler_ = LocationProfiler
        .WithTag("location_type", ToString(Type_))
        .WithTag("medium", GetMediumName(), -1)
        .WithTag("location_id", Id_, -1);

    PerformanceCounters_ = New<TLocationPerformanceCounters>(Profiler_);

    IOEngine_ = CreateIOEngine(
        Config_->IOEngineType,
        Config_->IOConfig,
        id,
        Profiler_,
        DataNodeLogger.WithTag("LocationId: %v", id));

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
        Bootstrap_->GetConfig()->DataNode->DiskHealthChecker,
        GetPath(),
        GetWritePoolInvoker(),
        DataNodeLogger,
        Profiler_);
}

const NChunkClient::IIOEnginePtr& TLocation::GetIOEngine() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return IOEngine_;
}

ELocationType TLocation::GetType() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Type_;
}

TLocationUuid TLocation::GetUuid() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Uuid_;
}

const TString& TLocation::GetMediumName() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Config_->MediumName;
}

const TMediumDescriptor& TLocation::GetMediumDescriptor() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto* descriptor = CurrentMediumDescriptor_.load();
    static const TMediumDescriptor Empty;
    return descriptor ? *descriptor : Empty;
}

void TLocation::SetMediumDescriptor(const TMediumDescriptor& descriptor)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = Guard(MediumDescriptorLock_);
    if (!MediumDescriptors_.empty() && descriptor == *MediumDescriptors_.back()) {
        return;
    }
    MediumDescriptors_.push_back(std::make_unique<TMediumDescriptor>(descriptor));
    CurrentMediumDescriptor_ = MediumDescriptors_.back().get();
}

const NProfiling::TRegistry& TLocation::GetProfiler() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Profiler_;
}

TLocationPerformanceCounters& TLocation::GetPerformanceCounters()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return *PerformanceCounters_;
}

const TString& TLocation::GetPath() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Config_->Path;
}

i64 TLocation::GetQuota() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Config_->Quota.value_or(std::numeric_limits<i64>::max());
}

i64 TLocation::GetCoalescedReadMaxGapSize() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Config_->CoalescedReadMaxGapSize;
}

const IInvokerPtr& TLocation::GetWritePoolInvoker()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return IOEngine_->GetWritePoolInvoker();
}

std::vector<TChunkDescriptor> TLocation::Scan()
{
    try {
        ValidateLockFile();
        ValidateMinimumSpace();
        ValidateWritable();
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Location disabled");
        MarkAsDisabled(ex);
        return std::vector<TChunkDescriptor>();
    }

    // Be optimistic and assume everything will be OK.
    // Also Disable requires Enabled_ to be true.
    Enabled_.store(true);

    try {
        return DoScan();
    } catch (const std::exception& ex) {
        Disable(TError("Location scan failed") << ex);
    }
}

void TLocation::Start()
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

void TLocation::Disable(const TError& reason)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!Enabled_.exchange(false)) {
        // Save only once.
        Sleep(TDuration::Max());
    }

    YT_LOG_ERROR(reason);

    // Save the reason in a file and exit.
    // Location will be disabled during the scan in the restart process.
    auto lockFilePath = NFS::CombinePaths(GetPath(), DisabledLockFileName);
    try {
        TFile file(lockFilePath, CreateAlways | WrOnly | Seq | CloseOnExec);
        TUnbufferedFileOutput fileOutput(file);
        fileOutput << ConvertToYsonString(reason, NYson::EYsonFormat::Pretty).AsStringBuf();
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Error creating location lock file");
        // Exit anyway.
    }

    YT_LOG_ERROR("Location is disabled; terminating");
    NLogging::TLogManager::Get()->Shutdown();
    _exit(1);
}

void TLocation::UpdateUsedSpace(i64 size)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!IsEnabled()) {
        return;
    }

    UsedSpace_ += size;
    AvailableSpace_ -= size;
}

i64 TLocation::GetUsedSpace() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return UsedSpace_.load();
}

i64 TLocation::GetAvailableSpace() const
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
        const_cast<TLocation*>(this)->Disable(error);
    }

    i64 remainingQuota = std::max(static_cast<i64>(0), GetQuota() - GetUsedSpace());
    availableSpace = std::min(availableSpace, remainingQuota);
    AvailableSpace_.store(availableSpace);

    return availableSpace;
}

i64 TLocation::GetPendingIOSize(
    EIODirection direction,
    const TWorkloadDescriptor& workloadDescriptor)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto category = ToIOCategory(workloadDescriptor);
    return PerformanceCounters_->PendingIOSize[direction][category].load();
}

i64 TLocation::GetMaxPendingIOSize(EIODirection direction)
{
    VERIFY_THREAD_AFFINITY_ANY();

    i64 result = 0;
    for (auto category : TEnumTraits<EIOCategory>::GetDomainValues()) {
        result = std::max(result, PerformanceCounters_->PendingIOSize[direction][category].load());
    }
    return result;
}

TPendingIOGuard TLocation::IncreasePendingIOSize(
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

EIOCategory TLocation::ToIOCategory(const TWorkloadDescriptor& workloadDescriptor)
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

void TLocation::DecreasePendingIOSize(
    EIODirection direction,
    EIOCategory category,
    i64 delta)
{
    VERIFY_THREAD_AFFINITY_ANY();

    UpdatePendingIOSize(direction, category, -delta);
}

void TLocation::UpdatePendingIOSize(
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

void TLocation::IncreaseCompletedIOSize(
    EIODirection direction,
    const TWorkloadDescriptor& workloadDescriptor,
    i64 delta)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto category = ToIOCategory(workloadDescriptor);
    PerformanceCounters_->CompletedIOSize[direction][category].Increment(delta);
}

void TLocation::UpdateSessionCount(ESessionType type, int delta)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!IsEnabled()) {
        return;
    }

    PerTypeSessionCount_[type] += delta;
}

int TLocation::GetSessionCount(ESessionType type) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return PerTypeSessionCount_[type];
}

int TLocation::GetSessionCount() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    int result = 0;
    for (const auto& count : PerTypeSessionCount_) {
        result += count.load();
    }
    return result;
}

void TLocation::UpdateChunkCount(int delta)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!IsEnabled()) {
        return;
    }

    ChunkCount_ += delta;
}

int TLocation::GetChunkCount() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return ChunkCount_;
}

TString TLocation::GetChunkPath(TChunkId chunkId) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return NFS::CombinePaths(GetPath(), GetRelativeChunkPath(chunkId));
}

void TLocation::RemoveChunkFilesPermanently(TChunkId chunkId)
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

void TLocation::RemoveChunkFiles(TChunkId chunkId, bool /*force*/)
{
    VERIFY_THREAD_AFFINITY_ANY();

    RemoveChunkFilesPermanently(chunkId);
}

IThroughputThrottlerPtr TLocation::GetOutThrottler(const TWorkloadDescriptor& descriptor) const
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

bool TLocation::IsReadThrottling()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto time = PerformanceCounters_->LastReadThrottleTime.load();
    return GetCpuInstant() < time + 2 * DurationToCpuDuration(Config_->ThrottleDuration);
}

bool TLocation::IsWriteThrottling()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto time = PerformanceCounters_->LastWriteThrottleTime.load();
    return GetCpuInstant() < time + 2 * DurationToCpuDuration(Config_->ThrottleDuration);
}

TString TLocation::GetRelativeChunkPath(TChunkId chunkId)
{
    int hashByte = chunkId.Parts32[0] & 0xff;
    return NFS::CombinePaths(Format("%02x", hashByte), ToString(chunkId));
}

void TLocation::ForceHashDirectories(const TString& rootPath)
{
    for (int hashByte = 0; hashByte <= 0xff; ++hashByte) {
        auto hashDirectory = Format("%02x", hashByte);
        NFS::MakeDirRecursive(NFS::CombinePaths(rootPath, hashDirectory), ChunkFilesPermissions);
    }
}

void TLocation::ValidateWritable()
{
    NFS::MakeDirRecursive(GetPath(), ChunkFilesPermissions);

    // Run first health check before to sort out read-only drives.
    WaitFor(HealthChecker_->RunCheck())
        .ThrowOnError();
}

void TLocation::InitializeCellId()
{
    auto cellIdPath = NFS::CombinePaths(GetPath(), CellIdFileName);
    if (NFS::Exists(cellIdPath)) {
        TUnbufferedFileInput file(cellIdPath);
        auto cellIdString = file.ReadAll();
        TCellId cellId;
        if (!TCellId::FromString(cellIdString, &cellId)) {
            THROW_ERROR_EXCEPTION("Failed to parse cell id %Qv",
                cellIdString);
        }
        if (cellId != Bootstrap_->GetCellId()) {
            THROW_ERROR_EXCEPTION("Wrong cell id: expected %v, found %v",
                Bootstrap_->GetCellId(),
                cellId);
        }
    } else {
        YT_LOG_INFO("Cell id file is not found, creating");
        TFile file(cellIdPath, CreateAlways | WrOnly | Seq | CloseOnExec);
        TUnbufferedFileOutput output(file);
        output.Write(ToString(Bootstrap_->GetCellId()));
    }
}

void TLocation::InitializeUuid()
{
    auto uuidPath = NFS::CombinePaths(GetPath(), LocationUuidFileName);
    if (NFS::Exists(uuidPath)) {
        TUnbufferedFileInput file(uuidPath);
        auto uuidString = file.ReadAll();
        if (!TCellId::FromString(uuidString, &Uuid_)) {
            THROW_ERROR_EXCEPTION("Failed to parse location uuid %Qv",
                uuidString);
        }
    } else {
        YT_LOG_INFO("Location uuid file is not found, creating");
        Uuid_ = TLocationUuid::Create();
        TFile file(uuidPath, CreateAlways | WrOnly | Seq | CloseOnExec);
        TUnbufferedFileOutput output(file);
        output.Write(ToString(Uuid_));
    }
}

bool TLocation::IsSick() const
{
    return IOEngine_->IsSick();
}

bool TLocation::TryLock(TChunkId chunkId, bool verbose)
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

void TLocation::Unlock(TChunkId chunkId)
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

void TLocation::OnHealthCheckFailed(const TError& error)
{
    Disable(error);
}

void TLocation::MarkAsDisabled(const TError& error)
{
    auto alert = TError("Chunk location at %v is disabled", GetPath())
        << error;
    auto masterConnector = Bootstrap_->GetMasterConnector();
    masterConnector->RegisterAlert(alert);

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

i64 TLocation::GetAdditionalSpace() const
{
    return 0;
}

bool TLocation::ShouldSkipFileName(const TString& fileName) const
{
    return
        fileName == CellIdFileName ||
        fileName == LocationUuidFileName;
}

std::vector<TChunkDescriptor> TLocation::DoScan()
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

void TLocation::DoStart()
{
    InitializeCellId();
    InitializeUuid();

    HealthChecker_->SubscribeFailed(BIND(&TLocation::OnHealthCheckFailed, Unretained(this)));
    HealthChecker_->Start();
}

////////////////////////////////////////////////////////////////////////////////

TStoreLocation::TStoreLocation(
    const TString& id,
    TStoreLocationConfigPtr config,
    TBootstrap* bootstrap)
    : TLocation(
        ELocationType::Store,
        id,
        config,
        bootstrap)
    , Config_(config)
    , JournalManager_(New<TJournalManager>(
        bootstrap->GetConfig()->DataNode,
        this,
        bootstrap))
    , TrashCheckQueue_(New<TActionQueue>(Format("Trash:%v", id)))
    , TrashCheckExecutor_(New<TPeriodicExecutor>(
        TrashCheckQueue_->GetInvoker(),
        BIND(&TStoreLocation::OnCheckTrash, MakeWeak(this)),
        Config_->TrashCheckPeriod))
{
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
    return std::nullopt;
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
        const auto& dispatcher = Bootstrap_->GetJournalDispatcher();
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

    return std::nullopt;
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
    if (TLocation::ShouldSkipFileName(fileName)) {
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
    auto result = TLocation::DoScan();

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
    TLocation::DoStart();

    JournalManager_->Initialize();

    TrashCheckExecutor_->Start();
}

////////////////////////////////////////////////////////////////////////////////

TCacheLocation::TCacheLocation(
    const TString& id,
    TCacheLocationConfigPtr config,
    TBootstrap* bootstrap)
    : TLocation(
        ELocationType::Cache,
        id,
        config,
        bootstrap)
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

    return std::nullopt;
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
    TLocationPtr owner)
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
