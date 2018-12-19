#include "location.h"
#include "private.h"
#include "blob_chunk.h"
#include "blob_reader_cache.h"
#include "config.h"
#include "journal_chunk.h"
#include "journal_dispatcher.h"
#include "journal_manager.h"
#include "master_connector.h"

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/cell_node/config.h>

#include <yt/server/hydra/changelog.h>
#include <yt/server/hydra/private.h>

#include <yt/server/misc/disk_health_checker.h>
#include <yt/server/misc/private.h>

#include <yt/ytlib/chunk_client/format.h>
#include <yt/ytlib/chunk_client/io_engine.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/misc/fs.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/logging/log_manager.h>

#include <yt/core/concurrency/thread_pool.h>

namespace NYT::NDataNode {

using namespace NChunkClient;
using namespace NCellNode;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NHydra;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

// Others must not be able to list chunk store and chunk cache directories.
static const int ChunkFilesPermissions = 0751;
static const auto TrashCheckPeriod = TDuration::Seconds(10);

////////////////////////////////////////////////////////////////////////////////

TLocation::TLocation(
    ELocationType type,
    const TString& id,
    TStoreLocationConfigBasePtr config,
    TBootstrap* bootstrap)
    : TDiskLocation(config, id, DataNodeLogger)
    , Bootstrap_(bootstrap)
    , Type_(type)
    , Id_(id)
    , Config_(config)
    , MetaReadQueue_(New<TActionQueue>(Format("MetaRead:%v", Id_)))
    , MetaReadInvoker_(CreatePrioritizedInvoker(MetaReadQueue_->GetInvoker()))
    , WriteThreadPool_(New<TThreadPool>(Bootstrap_->GetConfig()->DataNode->WriteThreadCount, Format("DataWrite:%v", Id_)))
    , WritePoolInvoker_(WriteThreadPool_->GetInvoker())
{
    auto* profileManager = NProfiling::TProfileManager::Get();
    Profiler_ = DataNodeProfiler.AddTags({
        profileManager->RegisterTag("location_id", Id_),
        profileManager->RegisterTag("location_type", Type_),
        profileManager->RegisterTag("medium", GetMediumName())
    });

    PerformanceCounters_.ThrottledReads = {"/throttled_reads", {}, config->ThrottleCounterInterval};
    PerformanceCounters_.ThrottledWrites = {"/throttled_writes", {}, config->ThrottleCounterInterval};
    PerformanceCounters_.PutBlocksWallTime = {"/put_blocks_wall_time", {}, NProfiling::EAggregateMode::All};
    PerformanceCounters_.BlobChunkMetaReadTime = {"/blob_chunk_meta_read_time", {}, NProfiling::EAggregateMode::All};
    PerformanceCounters_.BlobChunkReaderOpenTime = {"/blob_chunk_reader_open_time", {}, NProfiling::EAggregateMode::All};
    PerformanceCounters_.BlobBlockReadSize = {"/blob_block_read_size", {}, NProfiling::EAggregateMode::All};
    PerformanceCounters_.BlobBlockReadTime = {"/blob_block_read_time", {}, NProfiling::EAggregateMode::All};
    PerformanceCounters_.BlobBlockReadThroughput = {"/blob_block_read_throughput", {}, NProfiling::EAggregateMode::All};
    PerformanceCounters_.BlobBlockWriteSize = {"/blob_block_write_size", {}, NProfiling::EAggregateMode::All};
    PerformanceCounters_.BlobBlockWriteTime = {"/blob_block_write_time", {}, NProfiling::EAggregateMode::All};
    PerformanceCounters_.BlobBlockWriteThroughput = {"/blob_block_write_throughput", {}, NProfiling::EAggregateMode::All};
    PerformanceCounters_.JournalBlockReadSize = {"/journal_block_read_size", {}, NProfiling::EAggregateMode::All};
    PerformanceCounters_.JournalBlockReadTime = {"/journal_block_read_time", {}, NProfiling::EAggregateMode::All};
    PerformanceCounters_.JournalBlockReadThroughput = {"/journal_block_read_throughput", {}, NProfiling::EAggregateMode::All};
    PerformanceCounters_.JournalChunkCreateTime = {"/journal_chunk_create_time", {}, NProfiling::EAggregateMode::All};
    PerformanceCounters_.JournalChunkOpenTime = {"/journal_chunk_open_time", {}, NProfiling::EAggregateMode::All};
    PerformanceCounters_.JournalChunkOpenTime = {"/journal_chunk_remove_time", {}, NProfiling::EAggregateMode::All};
    for (auto type : TEnumTraits<ESessionType>::GetDomainValues()) {
        NProfiling::TTagIdList perTypeTagIds = {
            profileManager->RegisterTag("type", type)
        };
        PerformanceCounters_.SessionCount[type] = {"/session_count", perTypeTagIds};
    }
    PerformanceCounters_.AvailableSpace = {"/available_space", {}, NProfiling::EAggregateMode::All};
    PerformanceCounters_.Full = {"/full"};

    PerformanceCounters_.PendingIOSize.resize(
        TEnumTraits<EIODirection>::GetDomainSize() *
        TEnumTraits<EIOCategory>::GetDomainSize());
    PerformanceCounters_.CompletedIOSize.resize(
        TEnumTraits<EIODirection>::GetDomainSize() *
        TEnumTraits<EIOCategory>::GetDomainSize());

    IOEngine_ = CreateIOEngine(
        Config_->IOEngineType,
        Config_->IOConfig,
        id,
        Profiler_,
        NLogging::TLogger(DataNodeLogger).AddTag("LocationId: %v", id));

    auto throttlersProfiler = Profiler_.AppendPath("/location");

    auto createThrottler = [&] (const auto& config, const auto& name) {
        return CreateNamedReconfigurableThroughputThrottler(config, name, Logger, throttlersProfiler);
    };

    ReplicationOutThrottler_ = createThrottler(config->ReplicationOutThrottler, "ReplicationOutThrottler");
    TabletCompactionAndPartitioningOutThrottler_ = createThrottler(
        config->TabletCompactionAndPartitioningOutThrottler,
        "TabletCompactionAndPartitioningOutThrottler");
    TabletLoggingOutThrottler_ = createThrottler(config->TabletLoggingOutThrottler, "TabletLoggingOutThrottler");
    TabletPreloadOutThrottler_ = createThrottler(config->TabletPreloadOutThrottler, "TabletPreloadOutThrottler");
    TabletRecoveryOutThrottler_ = createThrottler(config->TabletRecoveryOutThrottler, "TabletRecoveryOutThrottler");
    UnlimitedOutThrottler_ = CreateNamedUnlimitedThroughputThrottler("UnlimitedOutThrottler", throttlersProfiler);

    HealthChecker_ = New<TDiskHealthChecker>(
        Bootstrap_->GetConfig()->DataNode->DiskHealthChecker,
        GetPath(),
        GetWritePoolInvoker(),
        DataNodeLogger,
        Profiler_);

    auto initializeCounters = [&] (const TString& path, auto getCounter) {
        for (auto direction : TEnumTraits<EIODirection>::GetDomainValues()) {
            for (auto category : TEnumTraits<EIOCategory>::GetDomainValues()) {
                auto& counter = (this->*getCounter)(direction, category);
                typedef typename std::remove_reference<decltype(counter)>::type TCounter;
                counter = TCounter(
                    path,
                    {
                        profileManager->RegisterTag("direction", direction),
                        profileManager->RegisterTag("category", category)
                    });
            }
        }
    };
    initializeCounters("/pending_data_size", &TLocation::GetPendingIOSizeCounter);
    initializeCounters("/blob_block_bytes", &TLocation::GetCompletedIOSizeCounter);
}

const NChunkClient::IIOEnginePtr& TLocation::GetIOEngine() const
{
    return IOEngine_;
}

ELocationType TLocation::GetType() const
{
    return Type_;
}

const TString& TLocation::GetId() const
{
    return Id_;
}

const TString& TLocation::GetMediumName() const
{
    return Config_->MediumName;
}

const TMediumDescriptor& TLocation::GetMediumDescriptor() const
{
    return MediumDescriptor_;
}

void TLocation::SetMediumDescriptor(const TMediumDescriptor& descriptor)
{
    MediumDescriptor_ = descriptor;
}

const NProfiling::TProfiler& TLocation::GetProfiler() const
{
    return Profiler_;
}

TLocationPerformanceCounters& TLocation::GetPerformanceCounters()
{
    return PerformanceCounters_;
}

TString TLocation::GetPath() const
{
    return Config_->Path;
}

i64 TLocation::GetQuota() const
{
    return Config_->Quota.value_or(std::numeric_limits<i64>::max());
}

IInvokerPtr TLocation::GetWritePoolInvoker()
{
    return WritePoolInvoker_;
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

    try {
        // Be optimistic and assume everything will be OK.
        // Also Disable requires Enabled_ to be true.
        Enabled_.store(true);
        return DoScan();
    } catch (const std::exception& ex) {
        Disable(TError("Location scan failed") << ex);
        Y_UNREACHABLE(); // Disable() exits the process.
    }
}

void TLocation::Start()
{
    if (!IsEnabled())
        return;

    try {
        DoStart();
    } catch (const std::exception& ex) {
        Disable(TError("Location start failed") << ex);
    }
}

void TLocation::Disable(const TError& reason)
{
    if (!Enabled_.exchange(false)) {
        // Save only once.
        Sleep(TDuration::Max());
    }

    YT_LOG_ERROR(reason);

    // Save the reason in a file and exit.
    // Location will be disabled during the scan in the restart process.
    auto lockFilePath = NFS::CombinePaths(GetPath(), DisabledLockFileName);
    try {
        auto errorData = ConvertToYsonString(reason, NYson::EYsonFormat::Pretty).GetData();
        TFile file(lockFilePath, CreateAlways | WrOnly | Seq | CloseOnExec);
        TUnbufferedFileOutput fileOutput(file);
        fileOutput << errorData;
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
    if (!IsEnabled())
        return;

    UsedSpace_ += size;
    AvailableSpace_ -= size;
}

i64 TLocation::GetUsedSpace() const
{
    return UsedSpace_;
}

i64 TLocation::GetAvailableSpace() const
{
    if (!IsEnabled()) {
        return 0;
    }

    auto path = GetPath();

    try {
        auto statistics = NFS::GetDiskSpaceStatistics(path);
        AvailableSpace_ = statistics.AvailableSpace + GetAdditionalSpace();
    } catch (const std::exception& ex) {
        auto error = TError("Failed to compute available space")
            << ex;
        const_cast<TLocation*>(this)->Disable(error);
        Y_UNREACHABLE(); // Disable() exits the process.
    }

    i64 remainingQuota = std::max(static_cast<i64>(0), GetQuota() - GetUsedSpace());
    AvailableSpace_ = std::min(AvailableSpace_, remainingQuota);

    return AvailableSpace_;
}

i64 TLocation::GetPendingIOSize(
    EIODirection direction,
    const TWorkloadDescriptor& workloadDescriptor)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto category = ToIOCategory(workloadDescriptor);
    return GetPendingIOSizeCounter(direction, category).GetCurrent();
}

i64 TLocation::GetMaxPendingIOSize(EIODirection direction)
{
    VERIFY_THREAD_AFFINITY_ANY();

    i64 result = 0;
    for (auto category : TEnumTraits<EIOCategory>::GetDomainValues()) {
        result = std::max(result, GetPendingIOSizeCounter(direction, category).GetCurrent());
    }
    return result;
}

TPendingIOGuard TLocation::IncreasePendingIOSize(
    EIODirection direction,
    const TWorkloadDescriptor& workloadDescriptor,
    i64 delta)
{
    VERIFY_THREAD_AFFINITY_ANY();

    Y_ASSERT(delta >= 0);
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

NProfiling::TSimpleGauge& TLocation::GetPendingIOSizeCounter(
    EIODirection direction,
    EIOCategory category)
{
    int index =
        static_cast<int>(direction) +
        TEnumTraits<EIODirection>::GetDomainSize() * static_cast<int>(category);
    return PerformanceCounters_.PendingIOSize[index];
}

NProfiling::TMonotonicCounter& TLocation::GetCompletedIOSizeCounter(
    EIODirection direction,
    EIOCategory category)
{
    int index =
        static_cast<int>(direction) +
        TEnumTraits<EIODirection>::GetDomainSize() * static_cast<int>(category);
    return PerformanceCounters_.CompletedIOSize[index];
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

    auto& counter = GetPendingIOSizeCounter(direction, category);
    i64 result = Profiler_.Increment(counter, delta);
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
    auto& counter = GetCompletedIOSizeCounter(direction, category);
    Profiler_.Increment(counter, delta);
}

void TLocation::UpdateSessionCount(ESessionType type, int delta)
{
    if (!IsEnabled()) {
        return;
    }

    PerTypeSessionCount_[type] += delta;
}

int TLocation::GetSessionCount(ESessionType type) const
{
    return PerTypeSessionCount_[type];
}

int TLocation::GetSessionCount() const
{
    int result = 0;
    for (auto count : PerTypeSessionCount_) {
        result += count;
    }
    return result;
}

void TLocation::UpdateChunkCount(int delta)
{
    if (!IsEnabled()) {
        return;
    }

    ChunkCount_ += delta;
}

int TLocation::GetChunkCount() const
{
    return ChunkCount_;
}

TString TLocation::GetChunkPath(TChunkId chunkId) const
{
    return NFS::CombinePaths(GetPath(), GetRelativeChunkPath(chunkId));
}

void TLocation::RemoveChunkFilesPermanently(TChunkId chunkId)
{
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
    } catch (const std::exception& ex) {
        auto error = TError(
            NChunkClient::EErrorCode::IOError,
            "Error removing chunk %v",
            chunkId)
            << ex;
        Disable(error);
        Y_UNREACHABLE(); // Disable() exits the process.
    }
}

void TLocation::RemoveChunkFiles(TChunkId chunkId, bool force)
{
    Y_UNUSED(force);
    RemoveChunkFilesPermanently(chunkId);
}

IThroughputThrottlerPtr TLocation::GetOutThrottler(const TWorkloadDescriptor& descriptor) const
{
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
    auto deadline = PerformanceCounters_.ThrottledReads.GetUpdateDeadline();
    return GetCpuInstant() < deadline + 2 * DurationToCpuDuration(Config_->ThrottleCounterInterval);
}

bool TLocation::IsWriteThrottling()
{
    auto deadline = PerformanceCounters_.ThrottledWrites.GetUpdateDeadline();
    return GetCpuInstant() < deadline + 2 * DurationToCpuDuration(Config_->ThrottleCounterInterval);
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

bool TLocation::IsSick() const
{
    return IOEngine_->IsSick();
}

void TLocation::OnHealthCheckFailed(const TError& error)
{
    Disable(error);
    Y_UNREACHABLE(); // Disable() exits the process.
}

void TLocation::MarkAsDisabled(const TError& error)
{
    auto alert = TError("Chunk location at %v is disabled", GetPath())
        << error;
    auto masterConnector = Bootstrap_->GetMasterConnector();
    masterConnector->RegisterAlert(alert);

    Enabled_.store(false);

    AvailableSpace_ = 0;
    UsedSpace_ = 0;
    PerTypeSessionCount_ = {};
    ChunkCount_ = 0;
}

i64 TLocation::GetAdditionalSpace() const
{
    return 0;
}

bool TLocation::ShouldSkipFileName(const TString& fileName) const
{
    // Skip cell_id file.
    if (fileName == CellIdFileName) {
        return true;
    }

    return false;
}

std::vector<TChunkDescriptor> TLocation::DoScan()
{
    YT_LOG_INFO("Scanning storage location");

    NFS::CleanTempFiles(GetPath());
    ForceHashDirectories(GetPath());

    THashSet<TChunkId> chunkIds;
    {
        // Enumerate files under the location's directory.
        // Note that these also include trash files but the latter are explicitly skipped.
        auto fileNames = NFS::EnumerateFiles(GetPath(), std::numeric_limits<int>::max());
        for (const auto& fileName : fileNames) {
            if (ShouldSkipFileName(fileName))
                continue;

            TChunkId chunkId;
            auto bareFileName = NFS::GetFileNameWithoutExtension(fileName);
            if (!TChunkId::FromString(bareFileName, &chunkId)) {
                YT_LOG_ERROR("Unrecognized file %v in location directory", fileName);
                continue;
            }

            chunkIds.insert(chunkId);
        }
    }

    // Construct the list of chunk descriptors.
    // Also "repair" half-alive chunks (e.g. those having some of their essential parts missing)
    // by moving them into trash.
    std::vector<TChunkDescriptor> descriptors;
    for (const auto& chunkId : chunkIds) {
        auto optionalDescriptor = RepairChunk(chunkId);
        if (optionalDescriptor) {
            descriptors.push_back(*optionalDescriptor);
        }
    }

    YT_LOG_INFO("Done, %v chunks found", descriptors.size());

    return descriptors;
}

void TLocation::DoStart()
{
    auto cellIdPath = NFS::CombinePaths(GetPath(), CellIdFileName);
    if (NFS::Exists(cellIdPath)) {
        TUnbufferedFileInput cellIdFile(cellIdPath);
        auto cellIdString = cellIdFile.ReadAll();
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
        TUnbufferedFileOutput cellIdFile(file);
        cellIdFile.Write(ToString(Bootstrap_->GetCellId()));
    }

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
        TrashCheckPeriod,
        EPeriodicExecutorMode::Automatic))
{
    auto throttlersProfiler = GetProfiler().AppendPath("/location");

    auto createThrottler = [&] (const auto& config, const auto& name) {
        return CreateNamedReconfigurableThroughputThrottler(config, name, Logger, throttlersProfiler);
    };

    RepairInThrottler_ = createThrottler(config->RepairInThrottler, "RepairInThrottler");
    ReplicationInThrottler_ = createThrottler(config->ReplicationInThrottler, "ReplicationInThrottler");
    TabletCompactionAndPartitioningInThrottler_ = createThrottler(
        config->TabletCompactionAndPartitioningInThrottler,
        "TabletCompactionAndPartitioningInThrottler");
    TabletLoggingInThrottler_ = createThrottler(config->TabletLoggingInThrottler, "TabletLoggingInThrottler");
    TabletSnapshotInThrottler_ = createThrottler(config->TabletSnapshotInThrottler, "TabletSnapshotInThrottler");
    TabletStoreFlushInThrottler_ = createThrottler(config->TabletStoreFlushInThrottler, "TabletStoreFlushInThrottler");
    UnlimitedInThrottler_ = CreateNamedUnlimitedThroughputThrottler("UnlimitedInThrottler", throttlersProfiler);
}

TJournalManagerPtr TStoreLocation::GetJournalManager()
{
    return JournalManager_;
}

i64 TStoreLocation::GetLowWatermarkSpace() const
{
    return Config_->LowWatermark;
}

bool TStoreLocation::IsFull() const
{
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
    return GetAvailableSpace() - size >= Config_->DisableWritesWatermark;
}

IThroughputThrottlerPtr TStoreLocation::GetInThrottler(const TWorkloadDescriptor& descriptor) const
{
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
    if (force) {
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
        TGuard<TSpinLock> guard(TrashMapSpinLock_);
        TrashMap_.insert(std::make_pair(timestamp, TTrashChunkEntry{chunkId, diskSpace}));
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
        Y_UNREACHABLE(); // Disable() exits the process.
    }
}

void TStoreLocation::CheckTrashTtl()
{
    auto deadline = TInstant::Now() - Config_->MaxTrashTtl;
    while (true) {
        TTrashChunkEntry entry;
        {
            TGuard<TSpinLock> guard(TrashMapSpinLock_);
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
        TGuard<TSpinLock> guard(TrashMapSpinLock_);
        // NB: Available space includes trash disk space.
        availableSpace = GetAvailableSpace() - TrashDiskSpace_;
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
            TGuard<TSpinLock> guard(TrashMapSpinLock_);
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
    } catch (const std::exception& ex) {
        auto error = TError(
            NChunkClient::EErrorCode::IOError,
            "Error moving chunk %v to trash",
            chunkId)
            << ex;
        Disable(error);
        Y_UNREACHABLE(); // Disable() exits the process.
    }
}

i64 TStoreLocation::GetAdditionalSpace() const
{
    // NB: Unguarded access to TrashDiskSpace_ seems OK.
    return TrashDiskSpace_;
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
            optionalDescriptor = RepairJournalChunk(chunkId);
            break;

        default:
            YT_LOG_WARNING("Invalid type %Qlv of chunk %v, skipped",
                chunkType,
                chunkId);
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
            return {
                primaryName,
                primaryName + "." + ChangelogIndexExtension,
                primaryName + "." + SealedFlagExtension
            };

        default:
            Y_UNREACHABLE();
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

    YT_LOG_INFO("Scanning storage trash");

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
                YT_LOG_ERROR("Unrecognized file %v in location trash directory", fileName);
                continue;
            }
            trashChunkIds.insert(chunkId);
        }

        for (const auto& chunkId : trashChunkIds) {
            RegisterTrashChunk(chunkId);
        }
    }

    YT_LOG_INFO("Done, %v trash chunks found", trashChunkIds.size());

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
        Profiler_.AppendPath("/cache")))
{ }

IThroughputThrottlerPtr TCacheLocation::GetInThrottler() const
{
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
            Y_UNREACHABLE();
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

TPendingIOGuard& TPendingIOGuard::operator=(TPendingIOGuard&& other)
{
    swap(*this, other);
    return *this;
}

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

void swap(TPendingIOGuard& lhs, TPendingIOGuard& rhs)
{
    using std::swap;
    swap(lhs.Direction_, rhs.Direction_);
    swap(lhs.Category_, rhs.Category_);
    swap(lhs.Size_, rhs.Size_);
    swap(lhs.Owner_, rhs.Owner_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
