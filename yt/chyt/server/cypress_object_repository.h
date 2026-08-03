#pragma once

#include "private.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/core/ypath/public.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <Interpreters/IExternalLoaderConfigRepository.h>

#include <Interpreters/StorageID.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

//! Cypress-backed catalog of clique objects.
//! The kind of an object is stored in the "chyt_object_type" attribute.
class TCypressObjectRepository
    : public TRefCounted
{
private:
    struct TObjectSnapshot;
    using TObjectSnapshotPtr = std::shared_ptr<TObjectSnapshot>;

public:
    static const std::string CypressConfigRepositoryName;
    static const std::string DictionaryObjectType;

    TCypressObjectRepository(
        NApi::NNative::IClientPtr client,
        TCypressObjectRepositoryConfigPtr config,
        IInvokerPtr invoker);

    void Start();

    void RefreshSnapshot();

    std::set<std::string> GetAllDictionaryNames();
    bool DictionaryExists(const std::string& dictionaryName);
    std::optional<DBPoco::Timestamp> GetDictionaryUpdateTime(const std::string& dictionaryName);

    DB::LoadablesConfigurationPtr LoadDictionary(const std::string& dictionaryName);
    std::optional<NHydra::TRevision> TryGetDictionaryRevision(const DB::StorageID& storageId);

    void WriteDictionary(
        const DB::ContextPtr& context,
        const DB::StorageID& storageId,
        const DB::LoadablesConfigurationPtr& config);
    void DeleteDictionary(
        const DB::ContextPtr& context,
        const DB::StorageID& storageId,
        NHydra::TRevision revision);

private:
    const NApi::NNative::IClientPtr Client_;
    const NYPath::TYPath RootPath_;

    NConcurrency::TPeriodicExecutorPtr SnapshotExecutor_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SnapshotLock_);
    TObjectSnapshotPtr Snapshot_;

    TObjectSnapshotPtr GetSnapshot();
    TObjectSnapshotPtr BuildSnapshot();

    NYPath::TYPath GetObjectPath(const std::string& objectName) const;
    static std::string GetObjectName(const DB::StorageID& storageId);
    void RemoveObject(
        const NApi::IClientPtr& client,
        const std::string& objectName,
        NHydra::TRevision revision);
};

DEFINE_REFCOUNTED_TYPE(TCypressObjectRepository)

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IExternalLoaderConfigRepository> CreateExternalLoaderFromCypressObjectRepository(TCypressObjectRepositoryPtr repository);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
