#include "cluster_resources.h"

#include <yt/server/cell_master/serialize.h>

#include <yt/server/security_server/security_manager.pb.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NSecurityServer {

using namespace NYson;

using NChunkClient::MaxMediumCount;
using NChunkServer::DefaultStoreMediumIndex;

////////////////////////////////////////////////////////////////////////////////

namespace {

void ValidateDiskSpace(i64 diskSpace)
{
    if (diskSpace < 0) {
        THROW_ERROR_EXCEPTION("Invalid disk space size: expected >= 0, found %v",
            diskSpace);
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TClusterResources::TClusterResources()
    : DiskSpace{}
    , NodeCount(0)
    , ChunkCount(0)
{ }

TClusterResources::TClusterResources(
    int nodeCount,
    int chunkCount)
    : DiskSpace{}
    , NodeCount(nodeCount)
    , ChunkCount(chunkCount)
{ }

void TClusterResources::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, DiskSpace);
    Save(context, NodeCount);
    Save(context, ChunkCount);
}

void TClusterResources::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    // COMPAT(shakurov)
    if (context.GetVersion() < 400) {
        DiskSpace[DefaultStoreMediumIndex] = Load<i64>(context);
    } else {
        Load(context, DiskSpace);
    }
    Load(context, NodeCount);
    Load(context, ChunkCount);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TClusterResources* protoResources, const TClusterResources& resources)
{
    protoResources->set_chunk_count(resources.ChunkCount);
    protoResources->set_node_count(resources.NodeCount);

    for (int index = 0; index < MaxMediumCount; ++index) {
        i64 diskSpace = resources.DiskSpace[index];
        if (diskSpace != 0) {
            auto* protoDiskSpace = protoResources->add_disk_space_per_medium();
            protoDiskSpace->set_medium_index(index);
            protoDiskSpace->set_disk_space(diskSpace);
        }
    }
}

void FromProto(TClusterResources* resources, const NProto::TClusterResources& protoResources)
{
    resources->ChunkCount = protoResources.chunk_count();
    resources->NodeCount = protoResources.node_count();

    std::fill_n(resources->DiskSpace, MaxMediumCount, 0);
    for (const auto& spaceStats : protoResources.disk_space_per_medium()) {
        resources->DiskSpace[spaceStats.medium_index()] = spaceStats.disk_space();
    }
}

////////////////////////////////////////////////////////////////////////////////

TSerializableClusterResources::TSerializableClusterResources()
{
    RegisterParameter("node_count", NodeCount_)
        .GreaterThanOrEqual(0);
    RegisterParameter("chunk_count", ChunkCount_)
        .GreaterThanOrEqual(0);
    RegisterParameter("disk_space_per_medium", DiskSpacePerMedium_);
    // NB: this is for (partial) compatibility: 'disk_space' is serialized when
    // read, but ignored when set. Hence no validation.
    RegisterParameter("disk_space", DiskSpace_)
        .Optional();

    RegisterValidator([&] {
        for (const auto& pair : DiskSpacePerMedium_) {
            ValidateDiskSpace(pair.second);
        }
    });
}

TSerializableClusterResources::TSerializableClusterResources(
    const NChunkServer::TChunkManagerPtr& chunkManager,
    const TClusterResources& clusterResources)
    : TSerializableClusterResources()
{
    NodeCount_ = clusterResources.NodeCount;
    ChunkCount_ = clusterResources.ChunkCount;
    DiskSpace_ = 0;
    for (const auto& pair : chunkManager->Media()) {
        const auto* medium = pair.second;
        if (medium->GetCache()) {
            continue;
        }
        int mediumIndex = medium->GetIndex();
        i64 mediumDiskSpace = clusterResources.DiskSpace[mediumIndex];
        YCHECK(DiskSpacePerMedium_.insert(std::make_pair(medium->GetName(), mediumDiskSpace)).second);
        DiskSpace_ += mediumDiskSpace;
    }
}

TClusterResources TSerializableClusterResources::ToClusterResources(const NChunkServer::TChunkManagerPtr& chunkManager) const
{
    TClusterResources result(NodeCount_, ChunkCount_);
    for (const auto& pair : DiskSpacePerMedium_) {
        auto* medium = chunkManager->GetMediumByNameOrThrow(pair.first);
        result.DiskSpace[medium->GetIndex()] = pair.second;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TClusterResources& operator += (TClusterResources& lhs, const TClusterResources& rhs)
{
    std::transform(
        std::begin(lhs.DiskSpace),
        std::end(lhs.DiskSpace),
        std::begin(rhs.DiskSpace),
        std::begin(lhs.DiskSpace),
        std::plus<i64>());
    lhs.NodeCount += rhs.NodeCount;
    lhs.ChunkCount += rhs.ChunkCount;
    return lhs;
}

TClusterResources operator + (const TClusterResources& lhs, const TClusterResources& rhs)
{
    auto result = lhs;
    result += rhs;
    return result;
}

TClusterResources& operator -= (TClusterResources& lhs, const TClusterResources& rhs)
{
    std::transform(
        std::begin(lhs.DiskSpace),
        std::end(lhs.DiskSpace),
        std::begin(rhs.DiskSpace),
        std::begin(lhs.DiskSpace),
        std::minus<i64>());
    lhs.NodeCount -= rhs.NodeCount;
    lhs.ChunkCount -= rhs.ChunkCount;
    return lhs;
}

TClusterResources operator - (const TClusterResources& lhs, const TClusterResources& rhs)
{
    auto result = lhs;
    result -= rhs;
    return result;
}

TClusterResources& operator *= (TClusterResources& lhs, i64 rhs)
{
    std::transform(
        std::begin(lhs.DiskSpace),
        std::end(lhs.DiskSpace),
        std::begin(lhs.DiskSpace),
        [&] (i64 space) { return space * rhs; });
    lhs.NodeCount *= rhs;
    lhs.ChunkCount *= rhs;
    return lhs;
}

TClusterResources operator * (const TClusterResources& lhs, i64 rhs)
{
    auto result = lhs;
    result *= rhs;
    return result;
}

TClusterResources operator -  (const TClusterResources& resources)
{
    TClusterResources result;
    std::transform(
        std::begin(resources.DiskSpace),
        std::end(resources.DiskSpace),
        std::begin(result.DiskSpace),
        std::negate<i64>());
    result.NodeCount = -resources.NodeCount;
    result.ChunkCount = -resources.ChunkCount;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT

