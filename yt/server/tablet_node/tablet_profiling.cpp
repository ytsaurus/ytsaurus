#include "private.h"
#include "tablet.h"
#include "tablet_profiling.h"

#include <yt/ytlib/chunk_client/data_statistics.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/core/profiling/profile_manager.h>
#include <yt/core/profiling/profiler.h>

#include <yt/core/misc/tls_cache.h>
#include <yt/core/misc/farm_hash.h>

namespace NYT {
namespace NTabletNode {

using namespace NProfiling;
using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

struct TUserTagTrait
{
    using TKey = TString;
    using TValue = TTagId;

    static const TString& ToKey(const TString& user)
    {
        return user;
    }

    static TTagId ToValue(const TString& user)
    {
        return TProfileManager::Get()->RegisterTag("user", user);
    }
};

TTagIdList AddUserTag(const TString& user, TTagIdList tags)
{
    tags.push_back(GetLocallyCachedValue<TUserTagTrait>(user));
    return tags;
}

////////////////////////////////////////////////////////////////////////////////

ui64 TTabletProfilerTraitBase::ToKey(const TTagIdList& list)
{
    /*
     * The magic is the following:
     * - front() returns tablet_id tag or table_name tag.
     * - back()  returns user tag or replica_id tag.
     *
     * Those 2 tags are unique to lookup appropriate counters for specific tablet.
     */
    return (static_cast<ui64>(list.front()) << 32) | (static_cast<ui64>(list.back()));
}

////////////////////////////////////////////////////////////////////////////////

TSimpleProfilerTraitBase::TKey TSimpleProfilerTraitBase::ToKey(const TTagIdList& list)
{
    // list.back() is user tag.
    return list.back();
}

////////////////////////////////////////////////////////////////////////////////

TListProfilerTraitBase::TKey TListProfilerTraitBase::ToKey(const TTagIdList& list)
{
    return list;
}

////////////////////////////////////////////////////////////////////////////////

struct TDiskBytesWrittenCounters
{
    TDiskBytesWrittenCounters(const TTagIdList& list)
        : DiskBytesWritten("/disk_bytes_written", list)
    { }

    TSimpleCounter DiskBytesWritten;
};

using TDiskBytesWrittenProfilerTrait = TListProfilerTrait<TDiskBytesWrittenCounters>;

void ProfileDiskPressure(
    TTabletSnapshotPtr tabletSnapshot,
    const TDataStatistics& dataStatistics,
    TTagId methodTag)
{
    auto diskSpace = CalculateDiskSpaceUsage(
        tabletSnapshot->WriterOptions->ReplicationFactor,
        dataStatistics.regular_disk_space(),
        dataStatistics.erasure_disk_space());
    auto tags = tabletSnapshot->DiskProfilerTags;
    tags.push_back(methodTag);
    auto& counters = GetLocallyGloballyCachedValue<TDiskBytesWrittenProfilerTrait>(tags);
    TabletNodeProfiler.Increment(counters.DiskBytesWritten, diskSpace);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

size_t hash<NYT::NTabletNode::TListProfilerTraitBase::TKey>::operator()(const NYT::NTabletNode::TListProfilerTraitBase::TKey& list) const
{
    size_t result = 1;
    for (auto tag : list) {
        result = NYT::FarmFingerprint(result, tag);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////
