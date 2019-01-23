#pragma once

#include <yt/server/clickhouse_server/public.h>

#include <memory>
#include <string>

namespace NYT::NClickHouseServer {

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
    IStoragePtr storage,
    IAuthorizationTokenPtr token,
    const std::string& path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
