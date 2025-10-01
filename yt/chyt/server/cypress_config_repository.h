#pragma once

#include "private.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <Interpreters/IExternalLoaderConfigRepository.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TCypressDictionaryConfigRepository
    : public TRefCounted
{
public:
    static const std::string CypressConfigRepositoryName;

    TCypressDictionaryConfigRepository(NApi::NNative::IClientPtr client, TDictionaryRepositoryConfigPtr config);

    std::set<std::string> GetAllDictionaryNames();
    bool DictionaryExists(const std::string& dictionaryName);
    DB::LoadablesConfigurationPtr LoadDictionary(const std::string& dictionaryName);

    void WriteDictionary(
        const DB::ContextPtr& context,
        const std::string& name,
        const DB::LoadablesConfigurationPtr& config);
    bool DeleteDictionary(
        const DB::ContextPtr& context,
        const std::string& name,
        const std::string& databaseName);

private:
    const NApi::NNative::IClientPtr Client_;
    const NYPath::TYPath PathToDictionaries_;

    NYPath::TYPath GetPathToConfig(const std::string& dictionaryName);
};

DEFINE_REFCOUNTED_TYPE(TCypressDictionaryConfigRepository)

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IExternalLoaderConfigRepository> CreateExternalLoaderFromCypressConfigRepository(TCypressDictionaryConfigRepositoryPtr cypressDictionaryConfigRepository);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
