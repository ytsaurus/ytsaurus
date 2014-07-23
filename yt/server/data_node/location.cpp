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

static const auto& Logger = DataNodeLogger;
static const int ChunkFilesPermissions = 0751;

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ELocationQueue,
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
    , ReadQueue_(New<TFairShareActionQueue>(Format("Read:%v", Id_), ELocationQueue::GetDomainNames()))
    , DataReadInvoker_(CreatePrioritizedInvoker(ReadQueue_->GetInvoker(ELocationQueue::Data)))
    , MetaReadInvoker_(CreatePrioritizedInvoker(ReadQueue_->GetInvoker(ELocationQueue::Meta)))
    , WriteQueue_(New<TThreadPool>(bootstrap->GetConfig()->DataNode->WriteThreadCount, Format("Write:%v", Id_)))
    , WriteInvoker_(WriteQueue_->GetInvoker())
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
        LOG_ERROR(ex, "Failed to compute available space");
        const_cast<TLocation*>(this)->Disable();
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
        LOG_ERROR(ex, "Failed to compute total space");
        const_cast<TLocation*>(this)->Disable();
        return 0;
    }
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
    ui8 firstHashByte = static_cast<ui8>(chunkId.Parts[0] & 0xff);
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

IInvokerPtr TLocation::GetWriteInvoker()
{
    return WriteInvoker_;
}

bool TLocation::IsEnabled() const
{
    return Enabled_.load();
}

void TLocation::Disable()
{
    if (Enabled_.exchange(false)) {
        ScheduleDisable();
    }
}

void TLocation::ScheduleDisable()
{
    Bootstrap_->GetControlInvoker()->Invoke(
        BIND(&TLocation::DoDisable, MakeStrong(this)));
}

void TLocation::DoDisable()
{
    LOG_ERROR("Location disabled");

    AvailableSpace_ = 0;
    UsedSpace_ = 0;
    SessionCount_ = 0;
    ChunkCount_ = 0;

    Disabled_.Fire();
}

std::vector<TChunkDescriptor> TLocation::Initialize()
{
    try {
        auto descriptors = DoInitialize();
        Enabled_.store(true);
        return descriptors;
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Location has failed to initialize");
        ScheduleDisable();
        return std::vector<TChunkDescriptor>();
    }
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
            THROW_ERROR_EXCEPTION("Min disk space requirement is not met: required %" PRId64 ", actual %" PRId64,
                minSpace,
                totalSpace);
        }
    }

    NFS::CleanTempFiles(path);

    yhash_set<TChunkId> chunkIds;
    auto fileNames = NFS::EnumerateFiles(path, std::numeric_limits<int>::max());
    for (const auto& fileName : fileNames) {
        if (fileName == CellGuidFileName)
            continue;

        TChunkId chunkId;
        auto strippedFileName = NFS::GetFileNameWithoutExtension(fileName);
        if (TChunkId::FromString(strippedFileName, &chunkId)) {
            chunkIds.insert(chunkId);
        } else {
            LOG_ERROR("Unrecognized file %s",
                ~fileName.Quote());
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
                LOG_WARNING("Invalid type %s of chunk %s, skipped",
                    ~FormatEnum(chunkType).Quote(),
                    ~ToString(chunkId));
                break;
        }

        if (maybeDescriptor) {
            descriptors.push_back(*maybeDescriptor);
        }
    }

    LOG_INFO("Done, %" PRISZT " chunks found", descriptors.size());

    auto cellGuidPath = NFS::CombinePaths(path, CellGuidFileName);
    if (NFS::Exists(cellGuidPath)) {
        TFileInput cellGuidFile(cellGuidPath);
        auto cellGuidString = cellGuidFile.ReadAll();
        TCellGuid cellGuid;
        if (!TGuid::FromString(cellGuidString, &cellGuid)) {
            THROW_ERROR_EXCEPTION("Failed to parse cell GUID %s",
                ~cellGuidString.Quote());
        }
        if (cellGuid != Bootstrap_->GetCellGuid()) {
            THROW_ERROR_EXCEPTION("Wrong cell GUID: expected %s, found %s",
                ~ToString(Bootstrap_->GetCellGuid()),
                ~ToString(cellGuid));
        }
    } else {
        LOG_INFO("Cell GUID file is not found, creating");
        TFile file(cellGuidPath, CreateAlways | WrOnly | Seq | CloseOnExec);
        TFileOutput cellGuidFile(file);
        cellGuidFile.Write(ToString(Bootstrap_->GetCellGuid()));
    }

    // Force subdirectories.
    for (int hashByte = 0; hashByte <= 0xff; ++hashByte) {
        NFS::ForcePath(NFS::CombinePaths(GetPath(), Format("%02x", hashByte)), ChunkFilesPermissions);
    }

    // Initialize and start health checker.
    HealthChecker_ = New<TDiskHealthChecker>(
        Bootstrap_->GetConfig()->DataNode->DiskHealthChecker,
        GetPath(),
        GetWriteInvoker());

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
            LOG_WARNING("Chunk meta file %s is empty",
                ~metaFileName.Quote());
            NFS::Remove(dataFileName);
            NFS::Remove(metaFileName);
            return Null;
        }

        TChunkDescriptor descriptor;
        descriptor.Id = chunkId;
        descriptor.Info.set_disk_space(dataSize + metaSize);
        return descriptor;
    }  if (!hasMeta && hasData) {
        LOG_WARNING("Missing meta file, removing data file %s",
            ~dataFileName.Quote());
        NFS::Remove(dataFileName);
        return Null;
    } else if (!hasData && hasMeta) {
        LOG_WARNING("Missing data file, removing meta file %s",
            ~dataFileName.Quote());
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
            LOG_WARNING("Missing data file, removing index file %s",
                ~indexFileName.Quote());
            NFS::Remove(indexFileName);
        }
        return Null;
    }

    auto dispatcher = Bootstrap_->GetJournalDispatcher();
    auto changelog = dispatcher->OpenChangelog(this, chunkId, false);

    TChunkDescriptor descriptor;
    descriptor.Id = chunkId;
    descriptor.Info.set_row_count(changelog->GetRecordCount());
    descriptor.Info.set_sealed(changelog->IsSealed());
    return descriptor;
}

void TLocation::OnHealthCheckFailed()
{
    switch (Type_) {
        case ELocationType::Store:
            Disable();
            break;
        case ELocationType::Cache:
            LOG_FATAL("Cache location has failed");
            break;
        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
