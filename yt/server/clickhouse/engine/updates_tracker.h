#pragma once

#include <yt/server/clickhouse/interop/api.h>

#include <memory>
#include <string>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

// TODO: more reliable api

class IUpdatesTracker
{
public:
    virtual ~IUpdatesTracker() = default;

    virtual bool IsModified() const = 0;
    virtual void FixCurrentVersion() = 0;
};

using IUpdatesTrackerPtr = std::unique_ptr<IUpdatesTracker>;

////////////////////////////////////////////////////////////////////////////////

IUpdatesTrackerPtr CreateUpdatesTracker(
    NInterop::IStoragePtr storage,
    NInterop::IAuthorizationTokenPtr token,
    const std::string& path);

} // namespace NClickHouse
} // namespace NYT
