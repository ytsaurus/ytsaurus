#pragma once

#include "private.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <Interpreters/IExternalLoaderConfigRepository.h>

#include <Interpreters/StorageID.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TCypressDictionaryConfigRepository
    : public TRefCounted
{
private:
    struct TDictionaryConfigSnapshot;
    using TDictionaryConfigSnapshotPtr = std::shared_ptr<TDictionaryConfigSnapshot>;

public:
    static const std::string CypressConfigRepositoryName;

    TCypressDictionaryConfigRepository(
        NApi::NNative::IClientPtr client,
        TDictionaryRepositoryConfigPtr config,
        IInvokerPtr invoker);

    void Start();

    void RefreshSnapshot();

    std::set<std::string> GetAllDictionaryNames();
    bool DictionaryExists(const std::string& dictionaryName);
    std::optional<DBPoco::Timestamp> GetDictionaryUpdateTime(const std::string& dictionaryName);

    DB::LoadablesConfigurationPtr LoadDictionary(const std::string& dictionaryName);

    void WriteDictionary(
        const DB::ContextPtr& context,
        const DB::StorageID& storageId,
        const DB::LoadablesConfigurationPtr& config);
    void DeleteDictionary(
        const DB::ContextPtr& context,
        const DB::StorageID& storageId);

private:
    const NApi::NNative::IClientPtr Client_;
    const NYPath::TYPath RootPath_;

    NConcurrency::TPeriodicExecutorPtr SnapshotExecutor_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SnapshotLock_);
    TDictionaryConfigSnapshotPtr Snapshot_;

    TDictionaryConfigSnapshotPtr GetSnapshot();
    TDictionaryConfigSnapshotPtr BuildSnapshot();

    NYPath::TYPath GetPathToConfig(const std::string& dictionaryName) const;
};

DEFINE_REFCOUNTED_TYPE(TCypressDictionaryConfigRepository)

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IExternalLoaderConfigRepository> CreateExternalLoaderFromCypressConfigRepository(TCypressDictionaryConfigRepositoryPtr cypressDictionaryConfigRepository);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
