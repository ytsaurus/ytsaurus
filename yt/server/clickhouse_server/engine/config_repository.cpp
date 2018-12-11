#include "config_repository.h"

#include "document_config.h"
#include "format_helpers.h"
#include "logging_helpers.h"
#include "type_helpers.h"

#include <yt/server/clickhouse_server/native/storage.h>

//#include <Poco/Logger.h>
//#include <Poco/Util/XMLConfiguration.h>

//#include <common/logger_useful.h>

#include <util/string/cast.h>

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

namespace {

////////////////////////////////////////////////////////////////////////////////

IConfigPtr LoadXmlConfigFromContent(const std::string& content)
{
    if (!content.empty()) {
        std::stringstream in(content);
        return new Poco::Util::XMLConfiguration(in);
    }
    return nullptr;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

// Effective polling through metadata cache

class TPoller
    : public IConfigPoller
{
private:
    NNative::IStoragePtr Storage;
    NNative::IAuthorizationTokenPtr Token;
    std::string ConfigPath;

public:
    TPoller(NNative::IStoragePtr storage,
                  NNative::IAuthorizationTokenPtr token,
                  std::string configPath)
        : Storage(std::move(storage))
        , Token(std::move(token))
        , ConfigPath(std::move(configPath))
    {}

    TMaybe<NNative::TRevision> GetRevision() const override
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
    NNative::IStoragePtr Storage;
    NNative::IAuthorizationTokenPtr Token;
    std::string ConfigsPath;

    Poco::Logger* Logger;

public:
    TConfigRepository(NNative::IStoragePtr storage,
                      NNative::IAuthorizationTokenPtr token,
                      std::string configsPath);

    std::string GetAddress() const override;

    bool Exists(const std::string& name) const override;
    std::vector<std::string> List() const override;
    NNative::TObjectAttributes GetAttributes(const std::string& name) const override;

    IConfigPtr Load(const std::string& name) const override;

    IConfigPollerPtr CreatePoller(const std::string& name) const override;

private:
    bool LooksLikeConfig(const NNative::TObjectAttributes& attributes) const;

    IConfigPtr LoadFromFile(const std::string& path) const;
    IConfigPtr LoadFromDocument(const std::string& path) const;

    std::string GetConfigPath(const std::string& name) const;
};

////////////////////////////////////////////////////////////////////////////////

TConfigRepository::TConfigRepository(NNative::IStoragePtr storage,
                                     NNative::IAuthorizationTokenPtr token,
                                     std::string configsPath)
    : Storage(std::move(storage))
    , Token(std::move(token))
    , ConfigsPath(std::move(configsPath))
    , Logger(&Poco::Logger::get("ConfigRepository"))
{
    CH_LOG_DEBUG(Logger, "Open configuration repository: " << Quoted(ConfigsPath));
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

NNative::TObjectAttributes TConfigRepository::GetAttributes(const std::string& name) const
{
    return Storage->GetObjectAttributes(*Token, ToString(GetConfigPath(name)));
}

bool TConfigRepository::LooksLikeConfig(const NNative::TObjectAttributes& attributes) const
{
    return static_cast<int>(attributes.Type) == static_cast<int>(NNative::EObjectType::Document) ||
           static_cast<int>(attributes.Type) == static_cast<int>(NNative::EObjectType::File);
}

IConfigPtr TConfigRepository::LoadFromFile(const std::string& path) const
{
    CH_LOG_INFO(Logger, "Loading configuration from file " << Quoted(path));

    std::string content;
    try {
        content = ToStdString(Storage->ReadFile(*Token, ToString(path)));
    } catch (...) {
        CH_LOG_WARNING(Logger, "Cannot read configuration file " << Quoted(path) << " from storage: " << CurrentExceptionText());
        return nullptr;
    }

    try {
        return LoadXmlConfigFromContent(content);
    } catch (...) {
        CH_LOG_WARNING(Logger, "Cannot parse content of configuration file " << Quoted(path) << ": " << CurrentExceptionText());
        return nullptr;
    }
}

IConfigPtr TConfigRepository::LoadFromDocument(const std::string& path) const
{
    CH_LOG_INFO(Logger, "Loading configuration from document " << Quoted(path));

    NNative::IDocumentPtr document;
    try {
        document = Storage->ReadDocument(*Token, ToString(path));
    } catch (...) {
        CH_LOG_WARNING(Logger, "Cannot read configuration document " << Quoted(path) << " from storage: " << CurrentExceptionText());
        return nullptr;
    }
    return CreateDocumentConfig(std::move(document));
}

IConfigPtr TConfigRepository::Load(const std::string& name) const
{
    const auto path = GetConfigPath(name);

    CH_LOG_DEBUG(Logger, "Loading configuration " << Quoted(name) << " from " << Quoted(path));

    NNative::TObjectAttributes attributes;
    try {
        attributes = Storage->GetObjectAttributes(*Token, ToString(path));
    } catch (...) {
        CH_LOG_WARNING(Logger, "Cannot get attributes of object " << Quoted(path) << " in storage: " << CurrentExceptionText());
        return nullptr;
    }

    switch (attributes.Type) {
        case NNative::EObjectType::File:
            return LoadFromFile(path);
        case NNative::EObjectType::Document:
            return LoadFromDocument(path);
        default:
            CH_LOG_WARNING(Logger,
                "Unexpected configuration object type: " << ToStdString(::ToString(static_cast<int>(attributes.Type))));
            return nullptr;
    }

    Y_UNREACHABLE();
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
    NNative::IStoragePtr storage,
    NNative::IAuthorizationTokenPtr token,
    const std::string& path)
{
    return std::make_shared<TConfigRepository>(
        std::move(storage),
        std::move(token),
        path);
}

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
