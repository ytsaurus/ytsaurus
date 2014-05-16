#include "stdafx.h"
#include "location.h"
#include "private.h"
#include "chunk.h"
#include "reader_cache.h"
#include "config.h"
#include "disk_health_checker.h"
#include "master_connector.h"

#include <core/misc/fs.h>

#include <core/ypath/token.h>

#include <ytlib/chunk_client/format.h>

#include <ytlib/election/public.h>

#include <server/cell_node/bootstrap.h>
#include <server/cell_node/config.h>

#include <util/folder/filelist.h>
#include <util/folder/dirut.h>

namespace NYT {
namespace NDataNode {

using namespace NChunkClient;
using namespace NYPath;
using namespace NCellNode;
using namespace NConcurrency;
using namespace NElection;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = DataNodeLogger;
static const int Permissions = 0751;

////////////////////////////////////////////////////////////////////////////////

namespace {

void RemoveFileOrThrow(const Stroka& fileName)
{
    if (!NFS::Remove(fileName)) {
        THROW_ERROR_EXCEPTION("Error deleting %s", ~fileName.Quote());
    }
}

} // namespace

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
    , Type(type)
    , Id(id)
    , Config(config)
    , Bootstrap(bootstrap)
    , Enabled(false)
    , AvailableSpace(0)
    , UsedSpace(0)
    , SessionCount(0)
    , ChunkCount(0)
    , ReadQueue(New<TFairShareActionQueue>(Sprintf("Read:%s", ~Id), ELocationQueue::GetDomainNames()))
    , DataReadInvoker(CreatePrioritizedInvoker(ReadQueue->GetInvoker(ELocationQueue::Data)))
    , MetaReadInvoker(CreatePrioritizedInvoker(ReadQueue->GetInvoker(ELocationQueue::Meta)))
    , WriteQueue(New<TThreadPool>(bootstrap->GetConfig()->DataNode->WriteThreadCount, Sprintf("Write:%s", ~Id)))
    , WriteInvoker(WriteQueue->GetInvoker())
    , Logger(DataNodeLogger)
{
    Logger.AddTag(Sprintf("Path: %s", ~Config->Path));
}

TLocation::~TLocation()
{ }

ELocationType TLocation::GetType() const
{
    return Type;
}

const Stroka& TLocation::GetId() const
{
    return Id;
}

void TLocation::UpdateUsedSpace(i64 size)
{
    if (!IsEnabled())
        return;

    UsedSpace += size;
    AvailableSpace -= size;
}

i64 TLocation::GetAvailableSpace() const
{
    if (!IsEnabled()) {
        return 0;
    }

    auto path = GetPath();

    try {
        auto statistics = NFS::GetDiskSpaceStatistics(path);
        AvailableSpace = statistics.AvailableSpace;
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Failed to compute available space");
        const_cast<TLocation*>(this)->Disable();
        AvailableSpace = 0;
        return 0;
    }

    i64 remainingQuota = std::max(static_cast<i64>(0), GetQuota() - GetUsedSpace());
    AvailableSpace = std::min(AvailableSpace, remainingQuota);

    return AvailableSpace;
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

TBootstrap* TLocation::GetBootstrap() const
{
    return Bootstrap;
}

i64 TLocation::GetUsedSpace() const
{
    return UsedSpace;
}

i64 TLocation::GetQuota() const
{
    return Config->Quota.Get(std::numeric_limits<i64>::max());
}

double TLocation::GetLoadFactor() const
{
    i64 used = GetUsedSpace();
    i64 quota = GetQuota();
    return used >= quota ? 1.0 : (double) used / quota;
}

Stroka TLocation::GetPath() const
{
    return Config->Path;
}

void TLocation::UpdateSessionCount(int delta)
{
    if (!IsEnabled())
        return;

    SessionCount += delta;
}

int TLocation::GetSessionCount() const
{
    return SessionCount;
}

void TLocation::UpdateChunkCount(int delta)
{
    if (!IsEnabled())
        return;

    ChunkCount += delta;
}

int TLocation::GetChunkCount() const
{
    return ChunkCount;
}

Stroka TLocation::GetChunkFileName(const TChunkId& chunkId) const
{
    ui8 firstHashByte = static_cast<ui8>(chunkId.Parts[0] & 0xff);
    return NFS::CombinePaths(
        GetPath(),
        Sprintf("%02x%s%s", firstHashByte, LOCSLASH_S, ~ToString(chunkId)));
}

bool TLocation::IsFull() const
{
    return GetAvailableSpace() < Config->LowWatermark;
}

bool TLocation::HasEnoughSpace(i64 size) const
{
    return GetAvailableSpace() - size >= Config->HighWatermark;
}

IPrioritizedInvokerPtr TLocation::GetDataReadInvoker()
{
    return DataReadInvoker;
}

IPrioritizedInvokerPtr TLocation::GetMetaReadInvoker()
{
    return MetaReadInvoker;
}

IInvokerPtr TLocation::GetWriteInvoker()
{
    return WriteInvoker;
}

bool TLocation::IsEnabled() const
{
    return Enabled.load();
}

void TLocation::Disable()
{
    if (Enabled.exchange(false)) {
        ScheduleDisable();
    }
}

void TLocation::ScheduleDisable()
{
    Bootstrap->GetControlInvoker()->Invoke(
        BIND(&TLocation::DoDisable, MakeStrong(this)));
}

void TLocation::DoDisable()
{
    LOG_ERROR("Location disabled");

    AvailableSpace = 0;
    UsedSpace = 0;
    SessionCount = 0;
    ChunkCount = 0;

    Disabled_.Fire();
}

std::vector<TChunkDescriptor> TLocation::Initialize()
{
    try {
        auto descriptors = DoInitialize();
        Enabled.store(true);
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
    NFS::ForcePath(path, Permissions);

    if (Config->MinDiskSpace) {
        i64 minSpace = Config->MinDiskSpace.Get();
        i64 totalSpace = GetTotalSpace();
        if (totalSpace < minSpace) {
            THROW_ERROR_EXCEPTION("Min disk space requirement is not met: required %" PRId64 ", actual %" PRId64,
                minSpace,
                totalSpace);
        }
    }

    NFS::CleanTempFiles(path);

    yhash_set<Stroka> recognizedFileNames;
    yhash_set<TChunkId> chunkIds;
    auto allFileNames = NFS::EnumerateFiles(path, std::numeric_limits<int>::max());
    for (const auto& fileName : allFileNames) {
        if (fileName == CellGuidFileName)
            continue;

        TChunkId chunkId;
        auto strippedFileName = NFS::GetFileNameWithoutExtension(fileName);
        if (TChunkId::FromString(strippedFileName, &chunkId)) {
            recognizedFileNames.insert(NFS::NormalizePathSeparators(NFS::CombinePaths(path, fileName)));
            chunkIds.insert(chunkId);
        } else {
            LOG_ERROR("Unrecognized file %s",
                ~fileName.Quote());
        }
    }

    std::vector<TChunkDescriptor> descriptors;
    for (const auto& chunkId : chunkIds) {
        auto chunkDataFileName = GetChunkFileName(chunkId);
        auto chunkMetaFileName = chunkDataFileName + ChunkMetaSuffix;

        bool hasMeta = recognizedFileNames.find(NFS::NormalizePathSeparators(chunkMetaFileName)) != recognizedFileNames.end();
        bool hasData = recognizedFileNames.find(NFS::NormalizePathSeparators(chunkDataFileName)) != recognizedFileNames.end();

        YCHECK(hasMeta || hasData);

        if (hasMeta && hasData) {
            i64 chunkDataSize = NFS::GetFileSize(chunkDataFileName);
            i64 chunkMetaSize = NFS::GetFileSize(chunkMetaFileName);
            if (chunkMetaSize == 0) {
                // EXT4 specific thing.
                // See https://bugs.launchpad.net/ubuntu/+source/linux/+bug/317781
                LOG_WARNING("Chunk meta file %s is empty",
                    ~chunkMetaFileName.Quote());
                RemoveFileOrThrow(chunkDataFileName);
                RemoveFileOrThrow(chunkMetaFileName);
                continue;
            }
            TChunkDescriptor descriptor;
            descriptor.Id = chunkId;
            descriptor.DiskSpace = chunkDataSize + chunkMetaSize;
            descriptors.push_back(descriptor);
        } else if (!hasMeta) {
            LOG_WARNING("Missing meta file, removing data file %s",
                ~chunkDataFileName.Quote());
            RemoveFileOrThrow(chunkDataFileName);
        } else if (!hasData) {
            LOG_WARNING("Missing data file, removing meta file %s",
                ~chunkMetaFileName.Quote());
            RemoveFileOrThrow(chunkMetaFileName);
        }
    }

    LOG_INFO("Done, %" PRISZT " chunks found", descriptors.size());

    auto cellGuidPath = NFS::CombinePaths(path, CellGuidFileName);
    if (isexist(~cellGuidPath)) {
        TFileInput cellGuidFile(cellGuidPath);
        auto cellGuidString = cellGuidFile.ReadAll();
        TCellGuid cellGuid;
        if (!TGuid::FromString(cellGuidString, &cellGuid)) {
            THROW_ERROR_EXCEPTION("Failed to parse cell GUID %s",
                ~cellGuidString.Quote());
        }
        if (cellGuid != Bootstrap->GetCellGuid()) {
            THROW_ERROR_EXCEPTION("Wrong cell GUID: expected %s, found %s",
                ~ToString(Bootstrap->GetCellGuid()),
                ~ToString(cellGuid));
        }
    } else {
        LOG_INFO("Cell GUID file is not found, creating");
        TFile file(cellGuidPath, CreateAlways | WrOnly | Seq | CloseOnExec);
        TFileOutput cellGuidFile(file);
        cellGuidFile.Write(ToString(Bootstrap->GetCellGuid()));
    }

    // Force subdirectories.
    for (int hashByte = 0; hashByte <= 0xff; ++hashByte) {
        NFS::ForcePath(NFS::CombinePaths(GetPath(), Sprintf("%02x", hashByte)), Permissions);
    }

    // Initialize and start health checker.
    HealthChecker = New<TDiskHealthChecker>(
        Bootstrap->GetConfig()->DataNode->DiskHealthChecker,
        GetPath(),
        GetWriteInvoker());

    // Run first health check before initialization is complete to sort out read-only drives.
    auto error = HealthChecker->RunCheck().Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(error);

    HealthChecker->SubscribeFailed(BIND(&TLocation::OnHealthCheckFailed, Unretained(this)));
    HealthChecker->Start();

    return descriptors;
}

TFuture<void> TLocation::ScheduleChunkRemoval(IChunk* chunk)
{
    const auto& id = chunk->GetId();

    Stroka dataFileName = GetChunkFileName(id);
    Stroka metaFileName = dataFileName + ChunkMetaSuffix;

    LOG_INFO("Chunk removal scheduled (ChunkId: %s)",
        ~ToString(id));

    auto promise = NewPromise();
    GetWriteInvoker()->Invoke(BIND([=] () mutable {
        LOG_DEBUG("Started removing chunk files (ChunkId: %s)",
            ~ToString(id));

        if (!NFS::Remove(dataFileName)) {
            LOG_ERROR("Failed to remove data file %s",
                ~dataFileName.Quote());
            Disable();
        }

        if (!NFS::Remove(metaFileName)) {
            LOG_ERROR("Failed to remove meta file %s",
                ~metaFileName.Quote());
            Disable();
        }

        LOG_DEBUG("Finished removing chunk files (ChunkId: %s)",
            ~ToString(id));
        promise.Set();
    }));

    return promise;
}

void TLocation::OnHealthCheckFailed()
{
    switch (Type) {
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
