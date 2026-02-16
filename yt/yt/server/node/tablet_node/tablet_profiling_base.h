#pragma once

#include "public.h"

#include <yt/yt/library/syncmap/map.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

template <class TCounter>
struct TUserTaggedCounter
{
    NConcurrency::TSyncMap<std::optional<std::string>, TCounter> Counters;

    TCounter* Get(
        bool disabled,
        const std::optional<std::string>& userTag,
        const NProfiling::TProfiler& profiler);
    TCounter* Get(
        bool disabled,
        const std::optional<std::string>& userTag,
        const NProfiling::TProfiler& tableProfiler,
        const NProfiling::TProfiler& mediumProfiler,
        const NProfiling::TProfiler& mediumHistogramProfiler,
        const NTableClient::TTableSchemaPtr& schema);
};

////////////////////////////////////////////////////////////////////////////////

class TTabletProfilerManager
{
public:
    TTabletProfilerManager();

    TTableProfilerPtr CreateTableProfiler(
        EDynamicTableProfilingMode profilingMode,
        const std::string& tabletCellBundle,
        const NYPath::TYPath& tablePath,
        const TString& tableTag,
        const std::string& account,
        const std::string& medium,
        NObjectClient::TObjectId schemaId,
        const NTableClient::TTableSchemaPtr& schema);

    THunkTabletProfilerPtr CreateHunkTabletProfiler(
        const std::string& tabletCellBundle,
        const NYPath::TYPath& hunkStoragePath,
        NTabletClient::TTabletId tabletId);

private:
    NThreading::TSpinLock Lock_;

    THashSet<TString> AllTables_;
    NProfiling::TGauge ConsumedTableTags_;

    using TProfilerKey = std::tuple<EDynamicTableProfilingMode, std::string, NYPath::TYPath, std::string, std::string, TObjectId>;
    THashMap<TProfilerKey, TWeakPtr<TTableProfiler>> Tables_;

    using THunkTabletProfilerKey = std::tuple<std::string, NYPath::TYPath, TObjectId>;
    THashMap<THunkTabletProfilerKey, TWeakPtr<THunkTabletProfiler>> HunkTabletProfilers_;
};

TTabletProfilerManager* GetTabletProfilerManager();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

#define TABLET_PROFILING_BASE_INL_H_
#include "tablet_profiling_base-inl.h"
#undef TABLET_PROFILING_BASE_INL_H_
