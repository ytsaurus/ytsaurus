#pragma once

#include <Dictionaries/IDictionary.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ExternalLoader.h>
#include <Common/quoteString.h>

#include <memory>

namespace DB
{

class IExternalLoaderConfigRepository;

/// Manages user-defined dictionaries.
class ExternalDictionariesLoader : public ExternalLoader, WithContext
{
public:
    using DictPtr = std::shared_ptr<const IDictionary>;

    /// Dictionaries will be loaded immediately and then will be updated in separate thread, each 'reload_period' seconds.
    explicit ExternalDictionariesLoader(ContextPtr global_context_);

    DictPtr getDictionary(const std::string & dictionary_name, ContextPtr context) const;

    DictPtr tryGetDictionary(const std::string & dictionary_name, ContextPtr context) const;

    void reloadDictionary(const std::string & dictionary_name, ContextPtr context) const;

    QualifiedTableName qualifyDictionaryNameWithDatabase(const std::string & dictionary_name, ContextPtr context) const;

    DictionaryStructure getDictionaryStructure(const std::string & dictionary_name, ContextPtr context) const;

    void assertDictionaryStructureExists(const std::string & dictionary_name, ContextPtr context) const;

    static DictionaryStructure getDictionaryStructure(const DBPoco::Util::AbstractConfiguration & config, const std::string & key_in_config = "dictionary");

    static DictionaryStructure getDictionaryStructure(const ObjectConfig & config);

    static void resetAll();

protected:
    LoadableMutablePtr createObject(const std::string & name, const DBPoco::Util::AbstractConfiguration & config,
                                    const std::string & key_in_config, const std::string & repository_name) const override;

    bool doesConfigChangeRequiresReloadingObject(const DBPoco::Util::AbstractConfiguration & old_config, const String & old_key_in_config,
                                                 const DBPoco::Util::AbstractConfiguration & new_config, const String & new_key_in_config) const override;

    void updateObjectFromConfigWithoutReloading(
        IExternalLoadable & object, const DBPoco::Util::AbstractConfiguration & config, const String & key_in_config) const override;

    std::string resolveDictionaryName(const std::string & dictionary_name, const std::string & current_database_name) const;

    /// Try convert qualified dictionary name to persistent UUID
    std::string resolveDictionaryNameFromDatabaseCatalog(const std::string & name, const std::string & current_database_name) const;

    friend class StorageSystemDictionaries;
    friend class DatabaseDictionary;
    friend class TableFunctionDictionary;
};

}
