#include "tablet_profiling_base.h"

#include "tablet.h"
#include "hunk_tablet_profiling.h"

namespace NYT::NTabletNode {

using namespace NProfiling;
using namespace NYPath;

namespace {

////////////////////////////////////////////////////////////////////////////////

TYPath SanitizeDigitsInYPath(const TYPath& path)
{
    auto pathCopy = path;
    for (auto& c : pathCopy) {
        if (std::isdigit(c)) {
            c = '_';
        }
    }
    return pathCopy;
}

TYPath GetTablePathFromProfilingTag(const std::string& tableTag)
{
    return "tag:" + tableTag;
}

void AddTableProfilingTags(
    TTagSet* tagSet,
    EProfilingTagExportMode profilingTagExportMode,
    const std::string& tableTag)
{
    auto tablePath = GetTablePathFromProfilingTag(tableTag);

    switch (profilingTagExportMode) {
        case EProfilingTagExportMode::TableTag:
            tagSet->AddTag({"table_tag", tableTag}, -1);
            break;

        case EProfilingTagExportMode::Both:
            tagSet->AddTag({"table_tag", tableTag}, -1);
            tagSet->AddExtensionTag({"table_path", tablePath}, -1);
            break;

        case EProfilingTagExportMode::TablePath:
            tagSet->AddTag({"table_path", tablePath}, -1);
            break;
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TTabletProfilerManager::TTabletProfilerManager()
    : ConsumedTableTags_(TabletNodeProfiler().Gauge("/consumed_table_tags"))
{ }

TTabletProfilerManager* TTabletProfilerManager::Get()
{
    return Singleton<TTabletProfilerManager>();
}

TTableProfilerPtr TTabletProfilerManager::CreateTableProfiler(
    EDynamicTableProfilingMode profilingMode,
    EProfilingTagExportMode profilingTagExportMode,
    const std::string& bundle,
    const NYPath::TYPath& tablePath,
    const std::string& tableTag,
    const std::string& account,
    const std::string& medium,
    NObjectClient::TObjectId schemaId,
    const NTableClient::TTableSchemaPtr& schema)
{
    auto guard = Guard(Lock_);

    auto profilingTagExportModeKey = profilingMode == EDynamicTableProfilingMode::Tag
        ? profilingTagExportMode
        : EProfilingTagExportMode::TableTag;

    auto constructProfilerKey = [&] (const std::string& tableIdentity) {
        return TProfilerKey{
            profilingMode,
            profilingTagExportModeKey,
            bundle,
            tableIdentity,
            account,
            medium,
            schemaId,
        };
    };

    TProfilerKey key;
    switch (profilingMode) {
        case EDynamicTableProfilingMode::Path:
            key = constructProfilerKey(tablePath);
            AllTables_.insert(tablePath);
            ConsumedTableTags_.Update(AllTables_.size());
            break;

        case EDynamicTableProfilingMode::Tag:
            key = constructProfilerKey(tableTag);
            if (profilingTagExportMode != EProfilingTagExportMode::TableTag) {
                AllTables_.insert(GetTablePathFromProfilingTag(tableTag));
                ConsumedTableTags_.Update(AllTables_.size());
            }
            break;

        case EDynamicTableProfilingMode::PathLetters:
            key = constructProfilerKey(SanitizeDigitsInYPath(tablePath));
            AllTables_.insert(SanitizeDigitsInYPath(tablePath));
            ConsumedTableTags_.Update(AllTables_.size());
            break;

        case EDynamicTableProfilingMode::Disabled:
        default:
            key = constructProfilerKey("");
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
            AddTableProfilingTags(&tableTagSet, profilingTagExportMode, tableTag);

            mediumTagSet = tableTagSet;
            mediumTagSet.AddTagWithChild({"medium", medium}, -1);

            diskTagSet = tableTagSet;
            diskTagSet.AddTagWithChild({"account", account}, -1);
            diskTagSet.AddTagWithChild({"medium", medium}, -2);
            break;

        case EDynamicTableProfilingMode::PathLetters:
            tableTagSet.AddTag({"table_path", SanitizeDigitsInYPath(tablePath)}, -1);

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
