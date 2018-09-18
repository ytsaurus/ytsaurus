#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/public.h>

#include <yt/core/profiling/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

// Adds user tag to specified tags and returns the resultant tag list.
NProfiling::TTagIdList AddUserTag(const TString& user, NProfiling::TTagIdList tags = {});

////////////////////////////////////////////////////////////////////////////////

// Trait to deal with complex tags. Example: several tags necessary to identify.
struct TListProfilerTraitBase
{
    using TKey = NProfiling::TTagIdList;

    static TKey ToKey(const NProfiling::TTagIdList& list);
};

////////////////////////////////////////////////////////////////////////////////

template <typename TBase, typename TCounters>
struct TProfilerTrait
    : public TBase
{
    using TValue = TCounters;

    static TValue ToValue(const NProfiling::TTagIdList& list)
    {
        return TValue{list};
    }
};

template <typename TCounters>
using TTabletProfilerTrait = TProfilerTrait<TListProfilerTraitBase, TCounters>;

////////////////////////////////////////////////////////////////////////////////

void ProfileChunkWriter(
    TTabletSnapshotPtr tabletSnapshot,
    const NChunkClient::NProto::TDataStatistics&,
    const NChunkClient::TCodecStatistics& codecStatistics,
    NProfiling::TTagId methodTag);

void ProfileChunkReader(
    TTabletSnapshotPtr tabletSnapshot,
    const NChunkClient::NProto::TDataStatistics& dataStatistics,
    const NChunkClient::TCodecStatistics& codecStatistics,
    const NChunkClient::TChunkReaderStatisticsPtr& chunkReaderStatistics,
    NProfiling::TTagId methodTag);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
