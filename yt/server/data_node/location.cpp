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

namespace NYT {
namespace NDataNode {

using namespace NChunkClient;
using namespace NYPath;
using namespace NCellNode;
using namespace NConcurrency;
using namespace NElection;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

static const int ChunkFilesPermissions = 0751;

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

Stroka TLocation::GetChunkFileName(const TChunkId& chunkId) const
{
    ui8 firstHashByte = static_cast<ui8>(chunkId.Parts32[0] & 0xff);
    return NFS::CombinePaths(
        GetPath(),
        Format("%02x%v%v", firstHashByte, LOCSLASH_S, chunkId));
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
    return result;
}

std::vector<TChunkDescriptor> TLocation::DoInitialize()
{
    auto path = GetPath();

    LOG_INFO("Scanning storage location");

    // Others must not be able to list chunk store and chunk cache dirs.
    NFS::ForcePath(path, ChunkFilesPermissions);

    if (Config_->MinDiskSpace) {
        i64 minSpace = Config_->MinDiskSpace.Get();
        i64 totalSpace = GetTotalSpace();
        if (totalSpace < minSpace) {
            THROW_ERROR_EXCEPTION("Min disk space requirement is not met: required %v, actual %v",
                minSpace,
                totalSpace);
        }
    }

    NFS::CleanTempFiles(path);

    yhash_set<TChunkId> chunkIds;
    auto fileNames = NFS::EnumerateFiles(path, std::numeric_limits<int>::max());
    for (const auto& fileName : fileNames) {
        if (fileName == CellIdFileName)
            continue;

        TChunkId chunkId;
        auto strippedFileName = NFS::GetFileNameWithoutExtension(fileName);
        if (TChunkId::FromString(strippedFileName, &chunkId)) {
            chunkIds.insert(chunkId);
        } else {
            LOG_ERROR("Unrecognized file %Qv", fileName);
        }
    }

    std::vector<TChunkDescriptor> descriptors;
    for (const auto& chunkId : chunkIds) {
        TNullable<TChunkDescriptor> maybeDescriptor;
        auto chunkType = TypeFromId(DecodeChunkId(chunkId).Id);
        switch (chunkType) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
                maybeDescriptor = TryGetBlobDescriptor(chunkId);
                break;

            case EObjectType::JournalChunk:
                maybeDescriptor = TryGetJournalDescriptor(chunkId);
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

    auto cellIdPath = NFS::CombinePaths(path, CellIdFileName);
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

    // Force subdirectories.
    for (int hashByte = 0; hashByte <= 0xff; ++hashByte) {
        NFS::ForcePath(NFS::CombinePaths(GetPath(), Format("%02x", hashByte)), ChunkFilesPermissions);
    }

    // Initialize and start health checker.
    HealthChecker_ = New<TDiskHealthChecker>(
        Bootstrap_->GetConfig()->DataNode->DiskHealthChecker,
        GetPath(),
        GetWritePoolInvoker());

    // Run first health check before initialization is complete to sort out read-only drives.
    auto error = HealthChecker_->RunCheck().Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(error);

    HealthChecker_->SubscribeFailed(BIND(&TLocation::OnHealthCheckFailed, Unretained(this)));
    HealthChecker_->Start();

    return descriptors;
}

TNullable<TChunkDescriptor> TLocation::TryGetBlobDescriptor(const TChunkId& chunkId)
{
    auto fileName = GetChunkFileName(chunkId);
    auto dataFileName = fileName;
    auto metaFileName = fileName + ChunkMetaSuffix;

    bool hasData = NFS::Exists(dataFileName);
    bool hasMeta = NFS::Exists(metaFileName);

    if (hasMeta && hasData) {
        i64 dataSize = NFS::GetFileSize(dataFileName);
        i64 metaSize = NFS::GetFileSize(metaFileName);
        if (metaSize == 0) {
            // EXT4 specific thing.
            // See https://bugs.launchpad.net/ubuntu/+source/linux/+bug/317781
            LOG_WARNING("Chunk meta file %Qv is empty", metaFileName);
            NFS::Remove(dataFileName);
            NFS::Remove(metaFileName);
            return Null;
        }

        TChunkDescriptor descriptor;
        descriptor.Id = chunkId;
        descriptor.DiskSpace = dataSize + metaSize;
        return descriptor;
    }  if (!hasMeta && hasData) {
        LOG_WARNING("Missing meta file, removing data file %v", dataFileName);
        NFS::Remove(dataFileName);
        return Null;
    } else if (!hasData && hasMeta) {
        LOG_WARNING("Missing data file, removing meta file %v", dataFileName);
        NFS::Remove(metaFileName);
        return Null;
    } else {
        // Has nothing :)
        return Null;
    }
}

TNullable<TChunkDescriptor> TLocation::TryGetJournalDescriptor(const TChunkId& chunkId)
{
    auto fileName = GetChunkFileName(chunkId);
    if (!NFS::Exists(fileName)) {
        auto indexFileName = fileName + "." + NHydra::ChangelogIndexExtension;
        if (NFS::Exists(indexFileName)) {
            LOG_WARNING("Missing data file, removing index file %v", indexFileName);
            NFS::Remove(indexFileName);
        }
        return Null;
    }

    auto dispatcher = Bootstrap_->GetJournalDispatcher();
    auto asyncChangelog = dispatcher->OpenChangelog(this, chunkId, false);

    auto changelogOrError = asyncChangelog.Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(changelogOrError);
    auto changelog = changelogOrError.Value();

    TChunkDescriptor descriptor;
    descriptor.Id = chunkId;
    descriptor.DiskSpace = changelog->GetDataSize();
    descriptor.Sealed = changelog->IsSealed();
    return descriptor;
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
