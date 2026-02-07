#include "tablet_profiling_base.h"

#include "tablet.h"
#include "hunk_tablet_profiling.h"

namespace NYT::NTabletNode {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TString HideDigits(const TString& path)
{
    TString pathCopy = path;
    for (auto& c : pathCopy) {
        if (std::isdigit(c)) {
            c = '_';
        }
    }
    return pathCopy;
}

////////////////////////////////////////////////////////////////////////////////

TTabletProfilerManager::TTabletProfilerManager()
    : ConsumedTableTags_(TabletNodeProfiler().Gauge("/consumed_table_tags"))
{ }

TTableProfilerPtr TTabletProfilerManager::CreateTableProfiler(
    EDynamicTableProfilingMode profilingMode,
    const std::string& bundle,
    const NYPath::TYPath& tablePath,
    const TString& tableTag,
    const std::string& account,
    const std::string& medium,
    NObjectClient::TObjectId schemaId,
    const NTableClient::TTableSchemaPtr& schema)
{
    auto guard = Guard(Lock_);

    TProfilerKey key;
    switch (profilingMode) {
        case EDynamicTableProfilingMode::Path:
            key = {profilingMode, bundle, tablePath, account, medium, schemaId};
            AllTables_.insert(tablePath);
            ConsumedTableTags_.Update(AllTables_.size());
            break;

        case EDynamicTableProfilingMode::Tag:
            key = {profilingMode, bundle, tableTag, account, medium, schemaId};
            break;

        case EDynamicTableProfilingMode::PathLetters:
            key = {profilingMode, bundle, HideDigits(tablePath), account, medium, schemaId};
            AllTables_.insert(HideDigits(tablePath));
            ConsumedTableTags_.Update(AllTables_.size());
            break;

        case EDynamicTableProfilingMode::Disabled:
        default:
            key = {profilingMode, bundle, "", account, medium, schemaId};
            break;
    }

    auto& profiler = Tables_[key];
    auto p = profiler.Lock();
    if (p) {
        return p;
    }

    TTagSet tableTagSet;
    tableTagSet.AddRequiredTag({"tablet_cell_bundle", bundle});

    TTagSet mediumTagSet = tableTagSet;
    TTagSet diskTagSet = tableTagSet;

    switch (profilingMode) {
        case EDynamicTableProfilingMode::Path:
            tableTagSet.AddTag({"table_path", tablePath}, -1);

            mediumTagSet = tableTagSet;
            mediumTagSet.AddTagWithChild({"medium", medium}, -1);

            diskTagSet = tableTagSet;
            diskTagSet.AddTagWithChild({"account", account}, -1);
            diskTagSet.AddTagWithChild({"medium", medium}, -2);
            break;

        case EDynamicTableProfilingMode::Tag:
            tableTagSet.AddTag({"table_tag", tableTag}, -1);

            mediumTagSet = tableTagSet;
            mediumTagSet.AddTagWithChild({"medium", medium}, -1);

            diskTagSet = tableTagSet;
            diskTagSet.AddTagWithChild({"account", account}, -1);
            diskTagSet.AddTagWithChild({"medium", medium}, -2);
            break;

        case EDynamicTableProfilingMode::PathLetters:
            tableTagSet.AddTag({"table_path", HideDigits(tablePath)}, -1);

            mediumTagSet = tableTagSet;
            mediumTagSet.AddTagWithChild({"medium", medium}, -1);

            diskTagSet = tableTagSet;
            diskTagSet.AddTagWithChild({"account", account}, -1);
            diskTagSet.AddTagWithChild({"medium", medium}, -2);
            break;

        case EDynamicTableProfilingMode::Disabled:
        default:
            mediumTagSet.AddTag({"medium", medium});

            diskTagSet.AddTag({"account", account});
            diskTagSet.AddTag({"medium", medium});
            break;
    }

    auto tableProfiler = TabletNodeProfiler()
        .WithHot()
        .WithSparse()
        .WithTags(tableTagSet);

    auto diskProfiler = TabletNodeProfiler()
        .WithHot()
        .WithSparse()
        .WithTags(diskTagSet);

    auto mediumProfiler = TabletNodeProfiler()
        .WithHot()
        .WithSparse()
        .WithTags(mediumTagSet);

    auto mediumHistogramProfiler = TabletNodeProfiler()
        .WithHot()
        .WithTag("tablet_cell_bundle", bundle)
        .WithTag("medium", medium);

    p = New<TTableProfiler>(tableProfiler, diskProfiler, mediumProfiler, mediumHistogramProfiler, schema);
    profiler = p;
    return p;
}

THunkTabletProfilerPtr TTabletProfilerManager::CreateHunkTabletProfiler(
    const std::string& bundle,
    const NYPath::TYPath& hunkStoragePath,
    TTabletId tabletId)
{
    auto guard = Guard(Lock_);

    AllTables_.insert(hunkStoragePath);
    ConsumedTableTags_.Update(AllTables_.size());

    THunkTabletProfilerKey key = {bundle, hunkStoragePath, tabletId};

    auto& cachedProfiler = HunkTabletProfilers_[key];
    if (auto lockedProfiler = cachedProfiler.Lock()) {
        return lockedProfiler;
    }

    TTagSet hunkStorageTagSet;
    hunkStorageTagSet.AddRequiredTag({"tablet_cell_bundle", bundle});
    hunkStorageTagSet.AddTag({"table_path", hunkStoragePath}, -1);
    hunkStorageTagSet.AddTag({"tablet_id", ToString(tabletId)}, -2);

    auto profiler = New<THunkTabletProfiler>(TabletNodeProfiler()
        .WithHot()
        .WithSparse()
        .WithTags(hunkStorageTagSet));
    cachedProfiler = profiler;

    return profiler;
}

TTabletProfilerManager* GetTabletProfilerManager()
{
    return Singleton<TTabletProfilerManager>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
