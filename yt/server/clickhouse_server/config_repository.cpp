#include "config_repository.h"

#include "document_config.h"
#include "format_helpers.h"
#include "logging_helpers.h"
#include "type_helpers.h"

#include <yt/server/clickhouse_server/storage.h>

#include <Poco/Logger.h>
#include <Poco/Util/XMLConfiguration.h>

#include <common/logger_useful.h>

#include <util/string/cast.h>

namespace NYT {
namespace NClickHouseServer {

class TPoller
    : public IConfigPoller
{
private:
    IStoragePtr Storage;
    IAuthorizationTokenPtr Token;
    std::string ConfigPath;

public:
    TPoller(IStoragePtr storage,
                  IAuthorizationTokenPtr token,
                  std::string configPath)
        : Storage(std::move(storage))
        , Token(std::move(token))
        , ConfigPath(std::move(configPath))
    {}

    std::optional<TRevision> GetRevision() const override
    {
        return Storage->GetObjectRevision(
            *Token,
            ToString(ConfigPath),
            /*throughCache=*/ true);
    }
};

////////////////////////////////////////////////////////////////////////////////

// Directory with documents/files

class TConfigRepository
    : public IConfigRepository
{
private:
    IStoragePtr Storage;
    IAuthorizationTokenPtr Token;
    std::string ConfigsPath;

    Poco::Logger* Logger;

public:
    TConfigRepository(IStoragePtr storage,
                      IAuthorizationTokenPtr token,
                      std::string configsPath);

    std::string GetAddress() const override;

    bool Exists(const std::string& name) const override;
    std::vector<std::string> List() const override;
    TObjectAttributes GetAttributes(const std::string& name) const override;

    IConfigPollerPtr CreatePoller(const std::string& name) const override;

private:
    bool LooksLikeConfig(const TObjectAttributes& attributes) const;

    std::string GetConfigPath(const std::string& name) const;
};

////////////////////////////////////////////////////////////////////////////////

TConfigRepository::TConfigRepository(IStoragePtr storage,
                                     IAuthorizationTokenPtr token,
                                     std::string configsPath)
    : Storage(std::move(storage))
    , Token(std::move(token))
    , ConfigsPath(std::move(configsPath))
    , Logger(&Poco::Logger::get("ConfigRepository"))
{
    LOG_DEBUG(Logger, "Open configuration repository: " << Quoted(ConfigsPath));
}

std::string TConfigRepository::GetAddress() const
{
    return ConfigsPath;
}

bool TConfigRepository::Exists(const std::string& name) const
{
    return Storage->Exists(*Token, ToString(GetConfigPath(name)));
}

std::vector<std::string> TConfigRepository::List() const
{
    auto objects = Storage->ListObjects(*Token, ToString(ConfigsPath));

    std::vector<std::string> names;
    names.reserve(objects.size());
    for (auto object : objects) {
        if (LooksLikeConfig(object.Attributes)) {
            names.push_back(ToStdString(object.Name));
        }
    }
    return names;
}

TObjectAttributes TConfigRepository::GetAttributes(const std::string& name) const
{
    return Storage->GetObjectAttributes(*Token, ToString(GetConfigPath(name)));
}

bool TConfigRepository::LooksLikeConfig(const TObjectAttributes& attributes) const
{
    return attributes.Type == EObjectType::Document ||
           attributes.Type == EObjectType::File;
}

IConfigPollerPtr TConfigRepository::CreatePoller(const std::string& name) const
{
    return std::make_unique<TPoller>(Storage, Token, GetConfigPath(name));
}

std::string TConfigRepository::GetConfigPath(const std::string& name) const
{
    auto path = Storage->PathService()->Build(ToString(ConfigsPath), {ToString(name)});
    return ToStdString(path);
}

////////////////////////////////////////////////////////////////////////////////

IConfigRepositoryPtr CreateConfigRepository(
    IStoragePtr storage,
    IAuthorizationTokenPtr token,
    const std::string& path)
{
    return std::make_shared<TConfigRepository>(
        std::move(storage),
        std::move(token),
        path);
}

} // namespace NClickHouseServer
} // namespace NYT
