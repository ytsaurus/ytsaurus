#pragma once

#include "private.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/ypath/public.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <Interpreters/IExternalLoaderConfigRepository.h>

#include <Interpreters/StorageID.h>

#include <Parsers/IAST_fwd.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ERepositoryObjectType,
    (Dictionary)
    (MaterializedView)
    (Unknown)
);

struct TRepositoryObjectDescriptor
{
    DB::StorageID StorageId;
    ERepositoryObjectType Type;
    NHydra::TRevision Revision;
};

////////////////////////////////////////////////////////////////////////////////

//! Cypress-backed catalog of clique objects.
class TCypressObjectRepository
    : public TRefCounted
{
private:
    struct TObjectSnapshot;
    using TObjectSnapshotPtr = std::shared_ptr<TObjectSnapshot>;

public:
    static const std::string CypressConfigRepositoryName;

    struct TMaterializedView
    {
        DB::ASTPtr CreateQuery;
        NYPath::TYPath SourcePath;
        NYPath::TYPath TargetPath;
        std::string Creator;
        std::string ObjectName;
        NObjectClient::TObjectId ObjectId;
        NObjectClient::TObjectId SourceObjectId;
        NObjectClient::TObjectId TargetObjectId;
        NHydra::TRevision Revision;
    };

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

    std::optional<TMaterializedView> TryGetMaterializedView(const DB::StorageID& storageId);
    std::vector<TMaterializedView> GetAllMaterializedViews();

    void WriteDictionary(
        const DB::ContextPtr& context,
        const DB::StorageID& storageId,
        const DB::LoadablesConfigurationPtr& config);

    void WriteMaterializedView(
        const DB::ContextPtr& context,
        const DB::StorageID& storageId,
        const TMaterializedViewConfiguration& config);

    void DeleteObject(
        const DB::ContextPtr& context,
        const TRepositoryObjectDescriptor& objectDescriptor);

private:
    const NApi::NNative::IClientPtr Client_;
    const NYPath::TYPath RootPath_;

    NConcurrency::TPeriodicExecutorPtr SnapshotExecutor_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SnapshotLock_);
    TObjectSnapshotPtr Snapshot_;

    TObjectSnapshotPtr GetSnapshot();
    TObjectSnapshotPtr BuildSnapshot();

    void DeleteDictionary(
        const DB::ContextPtr& context,
        const std::string& objectName,
        NHydra::TRevision revision);
    void DeleteMaterializedView(
        const DB::ContextPtr& context,
        const std::string& objectName,
        NHydra::TRevision revision);

    void RemoveObject(
        const NApi::IClientPtr& client,
        const std::string& objectName,
        NHydra::TRevision revision);

    NYPath::TYPath GetObjectPath(const std::string& objectName) const;
    static std::string GetObjectName(const DB::StorageID& storageId);
};

DEFINE_REFCOUNTED_TYPE(TCypressObjectRepository)

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IExternalLoaderConfigRepository> CreateExternalLoaderFromCypressObjectRepository(TCypressObjectRepositoryPtr repository);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
