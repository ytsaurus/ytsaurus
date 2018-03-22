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

// Trait to deal with simple tags. Example: only user tag or only tablet tags.
struct TSimpleProfilerTraitBase
{
    using TKey = NProfiling::TTagId;

    static TKey ToKey(const NProfiling::TTagIdList& list);
};

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
        return {list};
    }
};

template <typename TCounters>
using TSimpleProfilerTrait = TProfilerTrait<TSimpleProfilerTraitBase, TCounters>;

template <typename TCounters>
using TTabletProfilerTrait = TProfilerTrait<TListProfilerTraitBase, TCounters>;

template <typename TCounters>
using TListProfilerTrait = TProfilerTrait<TListProfilerTraitBase, TCounters>;

////////////////////////////////////////////////////////////////////////////////

void ProfileChunkWriter(
    TTabletSnapshotPtr tabletSnapshot,
    const NChunkClient::NProto::TDataStatistics&,
    const NChunkClient::TCodecStatistics& codecStatistics,
    NProfiling::TTagId methodTag);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

template <>
struct hash<NYT::NTabletNode::TListProfilerTraitBase::TKey>
{
    size_t operator()(const NYT::NTabletNode::TListProfilerTraitBase::TKey& list) const;
};

////////////////////////////////////////////////////////////////////////////////
