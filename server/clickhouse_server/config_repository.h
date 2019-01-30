#pragma once

#include "config.h"

#include <yt/server/clickhouse_server/public.h>
#include <yt/server/clickhouse_server/objects.h>

#include <Poco/Util/LayeredConfiguration.h>
#include <Poco/AutoPtr.h>

#include <memory>
#include <vector>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class IConfigPoller
{
public:
    virtual ~IConfigPoller() = default;

    virtual std::optional<TRevision> GetRevision() const = 0;
};

using IConfigPollerPtr = std::unique_ptr<IConfigPoller>;

////////////////////////////////////////////////////////////////////////////////

class IConfigRepository
{
public:
    virtual ~IConfigRepository() = default;

    virtual std::string GetAddress() const = 0;

    virtual bool Exists(const std::string& name) const = 0;
    virtual std::vector<std::string> List() const = 0;
    virtual TObjectAttributes GetAttributes(const std::string& name) const = 0;

    virtual IConfigPollerPtr CreatePoller(const std::string& name) const = 0;
};

using IConfigRepositoryPtr = std::shared_ptr<IConfigRepository>;

////////////////////////////////////////////////////////////////////////////////

IConfigRepositoryPtr CreateConfigRepository(
    IStoragePtr storage,
    IAuthorizationTokenPtr token,
    const std::string& path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
