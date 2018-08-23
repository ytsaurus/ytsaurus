#include "geo_dictionaries_loader.h"

#include "format_helpers.h"
#include "type_helpers.h"
#include "updates_tracker.h"

#include <Common/Exception.h>

#include <Dictionaries/Embedded/GeodataProviders/IHierarchiesProvider.h>
#include <Dictionaries/Embedded/GeodataProviders/INamesProvider.h>

#include <Dictionaries/Embedded/GeodataProviders/HierarchyFormatReader.h>
#include <Dictionaries/Embedded/GeodataProviders/NamesFormatReader.h>

#include <IO/ReadBufferFromMemory.h>

#include <util/string/cast.h>

#include <unordered_map>

// TODO: use NGeoBase::TLookupPtr in data sources/providers

namespace NYT {
namespace NClickHouse {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TStringReadBuffer
    : public DB::ReadBufferFromMemory
{
public:
    TStringReadBuffer(TString&& data)
        : DB::ReadBufferFromMemory(data.Data(), data.Size())
        , Data(std::move(data))
    {}

private:
    TString Data;
};

////////////////////////////////////////////////////////////////////////////////

void EnsureExists(
    NInterop::IStoragePtr storage,
    NInterop::IAuthorizationTokenPtr token,
    const std::string& path) {

    if (!storage->Exists(*token, ToString(path))) {
        throw Poco::Exception("Path not found: " + Quoted(path));
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TRegionsHierarchyDataSource
    : public IRegionsHierarchyDataSource
{
private:
    NInterop::IStoragePtr Storage;
    NInterop::IAuthorizationTokenPtr Token;
    std::string FilePath;

    IUpdatesTrackerPtr UpdatesTracker;

public:
    TRegionsHierarchyDataSource(
        NInterop::IStoragePtr storage,
        NInterop::IAuthorizationTokenPtr token,
        std::string path)
        : Storage(std::move(storage))
        , Token(std::move(token))
        , FilePath(std::move(path))
        , UpdatesTracker(CreateUpdatesTracker(Storage, Token, FilePath))
    {
        EnsureExists(Storage, Token, FilePath);
    }

    bool isModified() const override;
    IRegionsHierarchyReaderPtr createReader() override;
};

////////////////////////////////////////////////////////////////////////////////

bool TRegionsHierarchyDataSource::isModified() const
{
    return UpdatesTracker->IsModified();
}

IRegionsHierarchyReaderPtr TRegionsHierarchyDataSource::createReader()
{
    UpdatesTracker->FixCurrentVersion();
    auto data = Storage->ReadFile(*Token, ToString(FilePath));
    auto dataReader = std::make_shared<TStringReadBuffer>(std::move(data));
    return std::make_unique<RegionsHierarchyFormatReader>(std::move(dataReader));
}

////////////////////////////////////////////////////////////////////////////////

class TRegionsHierarchiesDataProvider
    : public IRegionsHierarchiesDataProvider
{
private:
    NInterop::IStoragePtr Storage;
    NInterop::IAuthorizationTokenPtr Token;
    std::string DirectoryPath;

    using THierarchyFiles = std::unordered_map<std::string, std::string>;
    THierarchyFiles HierarchyFiles;

public:
    TRegionsHierarchiesDataProvider(
        NInterop::IStoragePtr storage,
        NInterop::IAuthorizationTokenPtr token,
        std::string directoryPath)
        : Storage(std::move(storage))
        , Token(std::move(token))
        , DirectoryPath(std::move(directoryPath))
    {
        EnsureExists(Storage, Token, DirectoryPath);
        CheckDefaultHierarchy();
        DiscoverFilesWithCustomHierarchies();
    }

    std::vector<std::string> listCustomHierarchies() const override;

    IRegionsHierarchyDataSourcePtr getDefaultHierarchySource() const override;
    IRegionsHierarchyDataSourcePtr getHierarchySource(const std::string& name) const override;

private:
    std::string GetDefaultHierarchyFile() const;
    void CheckDefaultHierarchy() const;

    bool LooksLikeHierarchyFile(const NInterop::TObjectListItem object) const;
    void DiscoverFilesWithCustomHierarchies();
};

////////////////////////////////////////////////////////////////////////////////

std::vector<std::string> TRegionsHierarchiesDataProvider::listCustomHierarchies() const
{
    std::vector<std::string> names;
    for (auto& file : HierarchyFiles) {
        names.push_back(file.first);
    }
    return names;
}

IRegionsHierarchyDataSourcePtr TRegionsHierarchiesDataProvider::getDefaultHierarchySource() const
{
    return std::make_unique<TRegionsHierarchyDataSource>(
        Storage,
        Token,
        GetDefaultHierarchyFile());
}

IRegionsHierarchyDataSourcePtr TRegionsHierarchiesDataProvider::getHierarchySource(
    const std::string& name) const
{
    auto found = HierarchyFiles.find(name);
    if (found == HierarchyFiles.end()) {
        throw Poco::Exception("Regions hierarchy not found: " + Quoted(name));
    }
    return std::make_unique<TRegionsHierarchyDataSource>(Storage, Token, found->second);
}

std::string TRegionsHierarchiesDataProvider::GetDefaultHierarchyFile() const
{
    return DirectoryPath + "/default";
}

void TRegionsHierarchiesDataProvider::CheckDefaultHierarchy() const
{
    const auto defaultFile = GetDefaultHierarchyFile();
    if (!Storage->Exists(*Token, ToString(defaultFile))) {
        throw Poco::Exception("File with default regions hierarchy not found: " + defaultFile);
    }
    const auto attributes = Storage->GetObjectAttributes(*Token, ToString(defaultFile));
    if (attributes.Type != NInterop::EObjectType::File) {
        throw Poco::Exception(
            "File with default regions hierarchy has unexpected type: " +
            ToStdString(::ToString(attributes.Type)));
    }
}

bool TRegionsHierarchiesDataProvider::LooksLikeHierarchyFile(
    const NInterop::TObjectListItem object) const {
    return object.Attributes.Type == NInterop::EObjectType::File &&
           object.Name.size() == 2;
}

void TRegionsHierarchiesDataProvider::DiscoverFilesWithCustomHierarchies()
{
    auto objects = Storage->ListObjects(*Token, ToString(DirectoryPath));
    for (auto object : objects) {
        if (LooksLikeHierarchyFile(object)) {
            HierarchyFiles.emplace(object.Name, DirectoryPath + '/' + ToStdString(object.Name));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TLanguageRegionsNamesDataSource
    : public ILanguageRegionsNamesDataSource
{
private:
    NInterop::IStoragePtr Storage;
    NInterop::IAuthorizationTokenPtr Token;
    std::string FilePath;
    std::string Language;

    IUpdatesTrackerPtr UpdatesTracker;

public:
    TLanguageRegionsNamesDataSource(
        NInterop::IStoragePtr storage,
        NInterop::IAuthorizationTokenPtr token,
        std::string path,
        std::string language)
        : Storage(std::move(storage))
        , Token(std::move(token))
        , FilePath(std::move(path))
        , Language(std::move(language))
        , UpdatesTracker(CreateUpdatesTracker(Storage, Token, FilePath))
    {}

    bool isModified() const override;

    size_t estimateTotalSize() const override;

    ILanguageRegionsNamesReaderPtr createReader() override;

    std::string getLanguage() const override;

    std::string getSourceName() const override;
};

////////////////////////////////////////////////////////////////////////////////

bool TLanguageRegionsNamesDataSource::isModified() const
{
    return UpdatesTracker->IsModified();
}

size_t TLanguageRegionsNamesDataSource::estimateTotalSize() const
{
    // TODO
    const auto data = Storage->ReadFile(*Token, ToString(FilePath));
    return data.size();
}

ILanguageRegionsNamesReaderPtr TLanguageRegionsNamesDataSource::createReader()
{
    UpdatesTracker->FixCurrentVersion();
    auto data = Storage->ReadFile(*Token, ToString(FilePath));
    auto dataReader = std::make_shared<TStringReadBuffer>(std::move(data));
    return std::make_unique<LanguageRegionsNamesFormatReader>(std::move(dataReader));
}

std::string TLanguageRegionsNamesDataSource::getLanguage() const
{
    return Language;
}

std::string TLanguageRegionsNamesDataSource::getSourceName() const
{
    return "File " + FilePath;
}

////////////////////////////////////////////////////////////////////////////////

class TRegionsNamesDataProvider
    : public IRegionsNamesDataProvider
{
private:
    NInterop::IStoragePtr Storage;
    NInterop::IAuthorizationTokenPtr Token;
    std::string DirectoryPath;

public:
    TRegionsNamesDataProvider(
        NInterop::IStoragePtr storage,
        NInterop::IAuthorizationTokenPtr token,
        std::string directoryPath)
        : Storage(std::move(storage))
        , Token(std::move(token))
        , DirectoryPath(std::move(directoryPath))
    {
        EnsureExists(Storage, Token, DirectoryPath);
    }

    ILanguageRegionsNamesDataSourcePtr getLanguageRegionsNamesSource(
        const std::string& language) const override;

private:
    std::string GetDataFilePath(const std::string& language) const;
};

////////////////////////////////////////////////////////////////////////////////

ILanguageRegionsNamesDataSourcePtr TRegionsNamesDataProvider::getLanguageRegionsNamesSource(
    const std::string& language) const
{
    auto dataFilePath = GetDataFilePath(language);
    EnsureExists(Storage, Token, dataFilePath);
    return std::make_unique<TLanguageRegionsNamesDataSource>(
        Storage,
        Token,
        dataFilePath,
        language);
}

std::string TRegionsNamesDataProvider::GetDataFilePath(const std::string& language) const
{
    return DirectoryPath + "/" + language;
}

////////////////////////////////////////////////////////////////////////////////

class TGeoDictionariesLoader
    : public IGeoDictionariesLoader
{
private:
    NInterop::IStoragePtr Storage;
    NInterop::IAuthorizationTokenPtr Token;
    std::string GeodataPath;

public:
    TGeoDictionariesLoader(
        NInterop::IStoragePtr storage,
        NInterop::IAuthorizationTokenPtr token,
        std::string geodataPath)
        : Storage(std::move(storage))
        , Token(std::move(token))
        , GeodataPath(std::move(geodataPath))
    {}

    std::unique_ptr<RegionsHierarchies> reloadRegionsHierarchies(
        const Poco::Util::AbstractConfiguration & config) override;

    std::unique_ptr<RegionsNames> reloadRegionsNames(
        const Poco::Util::AbstractConfiguration & config) override;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<RegionsHierarchies> TGeoDictionariesLoader::reloadRegionsHierarchies(
    const Poco::Util::AbstractConfiguration & config)
{
    Y_UNUSED(config);

    auto dataProvider = std::make_unique<TRegionsHierarchiesDataProvider>(
        Storage,
        Token,
        GeodataPath + "/hierarchies");

    return std::make_unique<RegionsHierarchies>(std::move(dataProvider));
}

std::unique_ptr<RegionsNames> TGeoDictionariesLoader::reloadRegionsNames(
    const Poco::Util::AbstractConfiguration & config)
{
    Y_UNUSED(config);

    auto dataProvider = std::make_unique<TRegionsNamesDataProvider>(
        Storage,
        Token,
        GeodataPath + "/names");

    return std::make_unique<RegionsNames>(std::move(dataProvider));
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IGeoDictionariesLoader> CreateGeoDictionariesLoader(
    NInterop::IStoragePtr storage,
    NInterop::IAuthorizationTokenPtr authToken,
    const std::string& geodataPath)
{
    return std::make_unique<TGeoDictionariesLoader>(
        std::move(storage),
        std::move(authToken),
        geodataPath);
}

}   // namespace NClickHouse
}   // namespace NYT

