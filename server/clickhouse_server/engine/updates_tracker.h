#pragma once

#include <yt/server/clickhouse_server/native/public.h>

#include <memory>
#include <string>

namespace NYT::NClickHouseServer::NEngine {

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
    NNative::IStoragePtr storage,
    NNative::IAuthorizationTokenPtr token,
    const std::string& path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NEngine
