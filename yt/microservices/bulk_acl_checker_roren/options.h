#pragma once

#include <util/generic/string.h>

#include <optional>

struct TImportSnapshotOptions
{
    TString Cluster;
    TString Destination;
    TString SnapshotId;
    size_t SnapshotLimit;
    std::optional<TString> Pool;
    bool Force;
    size_t MemoryLimit;
};

struct TRemoveExcessiveOptions
{
    TString Cluster;
    TString Destination;
    i64 DaysToLeave;
};
