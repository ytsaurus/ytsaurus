#include "stdafx.h"
#include "location.h"
#include "private.h"
#include "blob_chunk.h"
#include "journal_chunk.h"
#include "blob_reader_cache.h"
#include "config.h"
#include "disk_health_checker.h"
#include "master_connector.h"
#include "journal_dispatcher.h"

#include <core/misc/fs.h>

#include <core/ypath/token.h>

#include <ytlib/chunk_client/format.h>

#include <ytlib/election/public.h>

#include <ytlib/object_client/helpers.h>

#include <server/hydra/changelog.h>
#include <server/hydra/private.h>

#include <server/cell_node/bootstrap.h>
#include <server/cell_node/config.h>
#include <search.h>

namespace NYT {
namespace NDataNode {

using namespace NChunkClient;
using namespace NYPath;
using namespace NCellNode;
using namespace NConcurrency;
using namespace NElection;
using namespace NObjectClient;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

// Others must not be able to list chunk store and chunk cache directories.
static const int ChunkFilesPermissions = 0751;

static const Stroka TrashDirectory("trash");
static const auto TrashCheckPeriod = TDuration::Seconds(10);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ELocationQueue,
    (Data)
    (Meta)
);

TLocation::TLocation(
    ELocationType type,
    const Stroka& id,
    TLocationConfigPtr config,
    TBootstrap* bootstrap)
    : Profiler_(DataNodeProfiler.GetPathPrefix() + "/" + ToYPathLiteral(id))
    , Type_(type)
    , Id_(id)
    , Config_(config)
    , Bootstrap_(bootstrap)
    , Enabled_(false)
    , AvailableSpace_(0)
    , UsedSpace_(0)
    , SessionCount_(0)
    , ChunkCount_(0)
    , ReadQueue_(New<TFairShareActionQueue>(Format("Read:%v", Id_), TEnumTraits<ELocationQueue>::GetDomainNames()))
    , DataReadInvoker_(CreatePrioritizedInvoker(ReadQueue_->GetInvoker(static_cast<int>(ELocationQueue::Data))))
    , MetaReadInvoker_(CreatePrioritizedInvoker(ReadQueue_->GetInvoker(static_cast<int>(ELocationQueue::Meta))))
    , WriteThreadPool_(New<TThreadPool>(bootstrap->GetConfig()->DataNode->WriteThreadCount, Format("Write:%v", Id_)))
    , WritePoolInvoker_(WriteThreadPool_->GetInvoker())
    , TrashCheckExecutor_(New<TPeriodicExecutor>(
        WritePoolInvoker_,
        BIND(&TLocation::OnCheckTrash, MakeWeak(this)),
        TrashCheckPeriod,
        EPeriodicExecutorMode::Manual))
    , Logger(DataNodeLogger)
{
    Logger.AddTag("Path: %v", Config_->Path);
}

TLocation::~TLocation()
{ }

ELocationType TLocation::GetType() const
{
    return Type_;
}

const Stroka& TLocation::GetId() const
{
    return Id_;
}

void TLocation::UpdateUsedSpace(i64 size)
{
    if (!IsEnabled())
        return;

    UsedSpace_ += size;
    AvailableSpace_ -= size;
}

i64 TLocation::GetAvailableSpace() const
{
    if (!IsEnabled()) {
        return 0;
    }

    auto path = GetPath();

    try {
        auto statistics = NFS::GetDiskSpaceStatistics(path);
        AvailableSpace_ = statistics.AvailableSpace;
    } catch (const std::exception& ex) {
        auto error = TError("Failed to compute available space") << ex;
        LOG_ERROR(error);
        const_cast<TLocation*>(this)->Disable(error);
        AvailableSpace_ = 0;
        return 0;
    }

    i64 remainingQuota = std::max(static_cast<i64>(0), GetQuota() - GetUsedSpace());
    AvailableSpace_ = std::min(AvailableSpace_, remainingQuota);

    return AvailableSpace_;
}

i64 TLocation::GetTotalSpace() const
{
    auto path = GetPath();
    try {
        auto statistics = NFS::GetDiskSpaceStatistics(path);
        return statistics.TotalSpace;
    } catch (const std::exception& ex) {
        auto error = TError("Failed to compute total space") << ex;
        LOG_ERROR(error);
        const_cast<TLocation*>(this)->Disable(error);
        return 0;
    }
}

i64 TLocation::GetLowWatermarkSpace() const
{
    return Config_->LowWatermark;
}

i64 TLocation::GetUsedSpace() const
{
    return UsedSpace_;
}

i64 TLocation::GetQuota() const
{
    return Config_->Quota.Get(std::numeric_limits<i64>::max());
}

double TLocation::GetLoadFactor() const
{
    i64 used = GetUsedSpace();
    i64 quota = GetQuota();
    return used >= quota ? 1.0 : (double) used / quota;
}

Stroka TLocation::GetPath() const
{
    return Config_->Path;
}

Stroka TLocation::GetTrashPath() const
{
    return NFS::CombinePaths(GetPath(), TrashDirectory);
}

Stroka TLocation::GetRelativeChunkPath(const TChunkId& chunkId)
{
    int hashByte = chunkId.Parts32[0] & 0xff;
    return Format("%02x/%v", hashByte, chunkId);
}

std::vector<Stroka> TLocation::GetChunkPartNames(const TChunkId& chunkId) const
{
    auto primaryName = ToString(chunkId);
    switch (TypeFromId(DecodeChunkId(chunkId).Id)) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
            return {primaryName, primaryName + ChunkMetaSuffix};
            break;

        case EObjectType::JournalChunk:
            return {primaryName, primaryName + ChangelogIndexSuffix};
            break;

        default:
            YUNREACHABLE();
    }
}

Stroka TLocation::GetChunkPath(const TChunkId& chunkId) const
{
    return NFS::CombinePaths(GetPath(), GetRelativeChunkPath(chunkId));
}

Stroka TLocation::GetTrashChunkPath(const TChunkId& chunkId) const
{
    return NFS::CombinePaths(GetTrashPath(), GetRelativeChunkPath(chunkId));
}

void TLocation::UpdateSessionCount(int delta)
{
    if (!IsEnabled())
        return;

    SessionCount_ += delta;
}

int TLocation::GetSessionCount() const
{
    return SessionCount_;
}

void TLocation::UpdateChunkCount(int delta)
{
    if (!IsEnabled())
        return;

    ChunkCount_ += delta;
}

int TLocation::GetChunkCount() const
{
    return ChunkCount_;
}

bool TLocation::IsFull() const
{
    return GetAvailableSpace() < Config_->LowWatermark;
}

bool TLocation::HasEnoughSpace(i64 size) const
{
    return GetAvailableSpace() - size >= Config_->HighWatermark;
}

IPrioritizedInvokerPtr TLocation::GetDataReadInvoker()
{
    return DataReadInvoker_;
}

IPrioritizedInvokerPtr TLocation::GetMetaReadInvoker()
{
    return MetaReadInvoker_;
}

IInvokerPtr TLocation::GetWritePoolInvoker()
{
    return WritePoolInvoker_;
}

bool TLocation::IsEnabled() const
{
    return Enabled_.load();
}

void TLocation::Disable(const TError& reason)
{
    if (Enabled_.exchange(false)) {
        ScheduleDisable(reason);
    }
}

void TLocation::ScheduleDisable(const TError& reason)
{
    Bootstrap_->GetControlInvoker()->Invoke(
        BIND(&TLocation::DoDisable, MakeStrong(this), reason));
}

void TLocation::DoDisable(const TError& reason)
{
    LOG_ERROR(reason, "Location disabled");

    AvailableSpace_ = 0;
    UsedSpace_ = 0;
    SessionCount_ = 0;
    ChunkCount_ = 0;

    TrashCheckExecutor_->Stop();

    Disabled_.Fire(reason);
}

std::vector<TChunkDescriptor> TLocation::Initialize()
{
    std::vector<TChunkDescriptor> result;

    try {
        result = DoInitialize();
        Enabled_.store(true);
    } catch (const std::exception& ex) {
        auto error = TError("Location has failed to initialize") << ex;
        LOG_ERROR(error);
        ScheduleDisable(error);
    }

    TrashCheckExecutor_->Start();

    return result;
}

std::vector<TChunkDescriptor> TLocation::DoInitialize()
{
    if (Config_->MinDiskSpace) {
        i64 minSpace = *Config_->MinDiskSpace;
        i64 totalSpace = GetTotalSpace();
        if (totalSpace < minSpace) {
            THROW_ERROR_EXCEPTION("Minimum disk space requirement is not met: required %v, actual %v",
                minSpace,
                totalSpace);
        }
    }

    LOG_INFO("Scanning storage location");

    NFS::ForcePath(GetPath(), ChunkFilesPermissions);
    NFS::ForcePath(GetTrashPath(), ChunkFilesPermissions);
    NFS::CleanTempFiles(GetPath());

    // Force subdirectories.
    for (int hashByte = 0; hashByte <= 0xff; ++hashByte) {
        auto hashDirectory = Format("%02x", hashByte);
        NFS::ForcePath(NFS::CombinePaths(GetPath(), hashDirectory), ChunkFilesPermissions);
        NFS::ForcePath(NFS::CombinePaths(GetTrashPath(), hashDirectory), ChunkFilesPermissions);
    }

    yhash_set<TChunkId> chunkIds;
    {
        // Enumerate files under the location's directory.
        // Note that these also include trash files but the latter are explicitly skipped.
        auto fileNames = NFS::EnumerateFiles(GetPath(), std::numeric_limits<int>::max());
        for (const auto& fileName : fileNames) {
            // Skip cell_id file.
            if (fileName == CellIdFileName)
                continue;

            // Skip trash directory.
            if (fileName.has_prefix(TrashDirectory + LOCSLASH_S))
                continue;

            TChunkId chunkId;
            auto bareFileName = NFS::GetFileNameWithoutExtension(fileName);
            if (!TChunkId::FromString(bareFileName, &chunkId)) {
                LOG_ERROR("Unrecognized file %v in location directory", fileName);
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
        TNullable<TChunkDescriptor> maybeDescriptor;
        auto chunkType = TypeFromId(DecodeChunkId(chunkId).Id);
        switch (chunkType) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
                maybeDescriptor = RepairBlobChunk(chunkId);
                break;

            case EObjectType::JournalChunk:
                maybeDescriptor = RepairJournalChunk(chunkId);
                break;

            default:
                LOG_WARNING("Invalid type %Qlv of chunk %v, skipped",
                    chunkType,
                    chunkId);
                break;
        }
        if (maybeDescriptor) {
            descriptors.push_back(*maybeDescriptor);
        }
    }

    LOG_INFO("Done, %v chunks found", descriptors.size());

    LOG_INFO("Scanning storage trash");

    yhash_set<TChunkId> trashChunkIds;
    {
        // Enumerate files under the location's trash directory.
        // Note that some of them might have just been moved there during repair.
        auto fileNames = NFS::EnumerateFiles(GetTrashPath(), std::numeric_limits<int>::max());

        for (const auto& fileName : fileNames) {
            TChunkId chunkId;
            auto bareFileName = NFS::GetFileNameWithoutExtension(fileName);
            if (!TChunkId::FromString(bareFileName, &chunkId)) {
                LOG_ERROR("Unrecognized file %v in location trash directory", fileName);
                continue;
            }
            trashChunkIds.insert(chunkId);
        }

        for (const auto& chunkId : trashChunkIds) {
            RegisterTrashChunk(chunkId);
        }
    }

    LOG_INFO("Done, %v trash chunks found", trashChunkIds.size());

    auto cellIdPath = NFS::CombinePaths(GetPath(), CellIdFileName);
    if (NFS::Exists(cellIdPath)) {
        TFileInput cellIdFile(cellIdPath);
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
        LOG_INFO("Cell id file is not found, creating");
        TFile file(cellIdPath, CreateAlways | WrOnly | Seq | CloseOnExec);
        TFileOutput cellIdFile(file);
        cellIdFile.Write(ToString(Bootstrap_->GetCellId()));
    }

    // Initialize and start health checker.
    HealthChecker_ = New<TDiskHealthChecker>(
        Bootstrap_->GetConfig()->DataNode->DiskHealthChecker,
        GetPath(),
        GetWritePoolInvoker());

    // Run first health check before initialization is complete to sort out read-only drives.
    HealthChecker_->RunCheck()
        .Get()
        .ThrowOnError();

    HealthChecker_->SubscribeFailed(BIND(&TLocation::OnHealthCheckFailed, Unretained(this)));
    HealthChecker_->Start();

    return descriptors;
}

TNullable<TChunkDescriptor> TLocation::RepairBlobChunk(const TChunkId& chunkId)
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
        LOG_WARNING("Chunk meta file %v is empty, removing chunk files",
            metaFileName);
        NFS::Remove(dataFileName);
        NFS::Remove(metaFileName);
    }  if (!hasMeta && hasData) {
        LOG_WARNING("Chunk meta file %v is missing, moving data file %v to trash",
            metaFileName,
            dataFileName);
        NFS::Replace(dataFileName, trashDataFileName);
    } else if (!hasData && hasMeta) {
        LOG_WARNING("Chunk data file %v is missing, moving meta file %v to trash",
            dataFileName,
            metaFileName);
        NFS::Replace(metaFileName, trashMetaFileName);
    }
    return Null;
}

TNullable<TChunkDescriptor> TLocation::RepairJournalChunk(const TChunkId& chunkId)
{
    auto fileName = GetChunkPath(chunkId);
    auto trashFileName = GetTrashChunkPath(chunkId);

    auto dataFileName = fileName;
    auto indexFileName = fileName + ChangelogIndexSuffix;

    auto trashDataFileName = trashFileName;
    auto trashIndexFileName = trashFileName + ChangelogIndexSuffix;

    bool hasData = NFS::Exists(dataFileName);
    bool hasIndex = NFS::Exists(indexFileName);

    if (hasData) {
        auto dispatcher = Bootstrap_->GetJournalDispatcher();
        // NB: This also creates the (possibly missing) index file.
        auto changelog = dispatcher->OpenChangelog(this, chunkId, false)
            .Get()
            .ValueOrThrow();
        TChunkDescriptor descriptor;
        descriptor.Id = chunkId;
        descriptor.DiskSpace = changelog->GetDataSize();
        descriptor.Sealed = changelog->IsSealed();
        return descriptor;
    } else if (!hasData && hasIndex) {
        LOG_WARNING("Journal data file %v is missing, moving index file %v to trash",
            dataFileName,
            indexFileName);
        NFS::Replace(indexFileName, trashIndexFileName);
    }

    return Null;
}

void TLocation::OnHealthCheckFailed(const TError& error)
{
    switch (Type_) {
        case ELocationType::Store:
            Disable(error);
            break;
        case ELocationType::Cache:
            LOG_FATAL(error, "Cache location has failed");
            break;
        default:
            YUNREACHABLE();
    }
}

void TLocation::RemoveChunkFiles(const TChunkId& chunkId)
{
    try {
        LOG_DEBUG("Started removing chunk files (ChunkId: %v)", chunkId);

        auto partNames = GetChunkPartNames(chunkId);
        auto directory = NFS::GetDirectoryName(GetChunkPath(chunkId));

        for (const auto& name : partNames) {
            auto fileName = NFS::CombinePaths(directory, name);
            NFS::Remove(fileName);
        }

        LOG_DEBUG("Finished removing chunk files (ChunkId: %v)", chunkId);
    } catch (const std::exception& ex) {
        auto error = TError(
            NChunkClient::EErrorCode::IOError,
            "Error removing chunk %v",
            chunkId)
            << ex;
        LOG_ERROR(error);
        Disable(error);
    }
}

void TLocation::MoveChunkFilesToTrash(const TChunkId& chunkId)
{
    try {
        LOG_DEBUG("Started moving chunk files to trash (ChunkId: %v)", chunkId);

        auto partNames = GetChunkPartNames(chunkId);
        auto directory = NFS::GetDirectoryName(GetChunkPath(chunkId));
        auto trashDirectory = NFS::GetDirectoryName(GetTrashChunkPath(chunkId));

        for (const auto& name : partNames) {
            auto srcFileName = NFS::CombinePaths(directory, name);
            auto dstFileName = NFS::CombinePaths(trashDirectory, name);
            NFS::Replace(srcFileName, dstFileName);
            NFS::Touch(dstFileName);
        }

        LOG_DEBUG("Finished moving chunk files to trash (ChunkId: %v)", chunkId);

        RegisterTrashChunk(chunkId);
    } catch (const std::exception& ex) {
        auto error = TError(
            NChunkClient::EErrorCode::IOError,
            "Error moving chunk %v to trash",
            chunkId)
            << ex;
        LOG_ERROR(error);
        Disable(error);
    }
}

void TLocation::RegisterTrashChunk(const TChunkId& chunkId)
{
    auto timestamp = TInstant::Zero();
    i64 diskSpace = 0;
    auto partNames = GetChunkPartNames(chunkId);
    for (const auto& name : partNames) {
        auto directory = NFS::GetDirectoryName(GetTrashChunkPath(chunkId));
        auto fileName = NFS::CombinePaths(directory, name);
        auto statistics = NFS::GetFileStatistics(fileName);
        timestamp = std::max(timestamp, statistics.ModificationTime);
        diskSpace += statistics.Size;
    }

    {
        TGuard<TSpinLock> guard(TrashMapSpinLock_);
        TrashMap_.insert(std::make_pair(timestamp, TTrashChunkEntry{chunkId, diskSpace}));
    }

    LOG_DEBUG("Trash chunk registered (ChunkId: %v, Timestamp: %v, DiskSpace: %v)",
        chunkId,
        timestamp,
        diskSpace);
}

void TLocation::OnCheckTrash()
{
    if (!IsEnabled())
        return;

    try {
        CheckTrashTtl();
        CheckTrashWatermark();
        TrashCheckExecutor_->ScheduleNext();
    } catch (const std::exception& ex) {
        auto error = TError(ex);
        LOG_ERROR(error);
        Disable(error);
    }
}

void TLocation::CheckTrashTtl()
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
        }
        RemoveTrashFiles(entry);
    }
}

void TLocation::CheckTrashWatermark()
{
    i64 availableSpace;
    auto beginCleanup = [&] () {
        TGuard<TSpinLock> guard(TrashMapSpinLock_);
        availableSpace = GetAvailableSpace();
        return availableSpace < Config_->TrashCleanupWatermark && !TrashMap_.empty();
    };

    if (!beginCleanup())
        return;

    LOG_INFO("Low available disk space, starting trash cleanup (AvailableSpace: %v)",
        availableSpace);

    while (beginCleanup()) {
        while (true) {
            TTrashChunkEntry entry;
            {
                TGuard<TSpinLock> guard(TrashMapSpinLock_);
                if (TrashMap_.empty())
                    break;
                auto it = TrashMap_.begin();
                entry = it->second;
                TrashMap_.erase(it);
            }
            RemoveTrashFiles(entry);
            availableSpace += entry.DiskSpace;
        }
    }

    LOG_INFO("Finished trash cleanup (AvailableSpace: %v)",
        availableSpace);
}

void TLocation::RemoveTrashFiles(const TTrashChunkEntry& entry)
{
    auto partNames = GetChunkPartNames(entry.ChunkId);
    for (const auto& name : partNames) {
        auto directory = NFS::GetDirectoryName(GetTrashChunkPath(entry.ChunkId));
        auto fileName = NFS::CombinePaths(directory, name);
        if (NFS::Exists(fileName)) {
            NFS::Remove(fileName);
        }
    }

    LOG_DEBUG("Trash chunk removed (ChunkId: %v, DiskSpace: %v)",
        entry.ChunkId,
        entry.DiskSpace);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
