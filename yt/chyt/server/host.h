#pragma once

#include "cluster_nodes.h"
#include "private.h"
#include "config.h"
#include "cypress_config_repository.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/client/security_client/acl.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/ytree/permission.h>

#include <string>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class THost
    : public TRefCounted
{
public:
    using TRowLevelAcl = std::optional<std::vector<NSecurityClient::TRowLevelAccessControlEntry>>;

    THost(
        IInvokerPtr controlInvoker,
        TPorts ports,
        TYtConfigPtr config,
        NApi::NNative::TConnectionCompoundConfigPtr connectionConfig);

    virtual ~THost() override;

    void Start();

    void HandleIncomingGossip(const TString& instanceId, EInstanceState state);

    TFuture<void> StopDiscovery();

    void ValidateCliquePermission(const TString& user, NYTree::EPermission permission) const;

    std::vector<TRowLevelAcl> ValidateTableReadPermissionsAndGetRowLevelAcl(
        const std::vector<NYPath::TRichYPath>& paths,
        const TString& user);

    TFuture<std::vector<TErrorOr<EPreliminaryCheckPermissionResult>>> PreliminaryCheckPermissions(
        const std::vector<NYPath::TYPath>& paths,
        const TString& user);
    //! Get the names of the attributes that the local cache is configured for.
    //! NB: This is the state of THost, not some global variable,
    //! because the set of attributes may depend on the config.
    const std::vector<std::string>& GetObjectAttributeNamesToFetch() const;

    //! Get object attributes via local cache.
    std::vector<TErrorOr<NYTree::IAttributeDictionaryPtr>> GetObjectAttributes(
        const std::vector<NYPath::TYPath>& paths,
        const NApi::NNative::IClientPtr& client);
    //! Invalidate object attribute entries in local cache.
    void InvalidateCachedObjectAttributes(
        const std::vector<std::pair<NYPath::TYPath, NHydra::TRevision>>& paths);
    //! Invalidate object attribute entries on the whole clique via rpc requests.
    void InvalidateCachedObjectAttributesGlobally(
        const std::vector<std::pair<NYPath::TYPath, NHydra::TRevision>>& paths,
        EInvalidateCacheMode mode,
        TDuration timeout);

    const TTableSchemaCachePtr& GetTableSchemaCache() const;

    const NObjectClient::TObjectAttributeCachePtr& GetObjectAttributeCache() const;

    const IInvokerPtr& GetControlInvoker() const;

    //! Thread pool for heavy stuff.
    const IInvokerPtr& GetWorkerInvoker() const;

    //! Wrapper around previous thread pool which does bookkeeping around
    //! DB::current_thread.
    //!
    //! Cf. clickhouse_invoker.h
    const IInvokerPtr& GetClickHouseWorkerInvoker() const;

    //! Thread pool for input fetching.
    const IInvokerPtr& GetFetcherInvoker() const;

    //! Wrapper around previous thread pool which does bookkeeping around
    //! DB::current_thread.
    //!
    //! Cf. clickhouse_invoker.h
    const IInvokerPtr& GetClickHouseFetcherInvoker() const;

    NApi::NNative::IClientPtr GetRootClient() const;
    NApi::NNative::IClientPtr CreateClient(const std::string& user) const;

    //! Return nodes available through discovery service.
    //! In some cases local node can be out of discovery protocol
    //! (e.g. the instance is in 'interrupting' state or not started yet).
    //! |alwaysIncludeLocal| controls the behavior in such cases.
    TClusterNodes GetNodes(bool alwaysIncludeLocal = false) const;
    IClusterNodePtr GetLocalNode() const;

    int GetInstanceCookie() const;

    const NChunkClient::IMultiReaderMemoryManagerPtr& GetMultiReaderMemoryManager() const;

    TYtConfigPtr GetConfig() const;

    EInstanceState GetInstanceState() const;

    void HandleCrashSignal() const;
    void HandleSigint();

    TQueryRegistryPtr GetQueryRegistry() const;

    //! Return future which is set when no query is executing.
    TFuture<void> GetIdleFuture() const;

    void PopulateSystemDatabase(DB::IDatabase* systemDatabase) const;
    DB::DatabasePtr CreateYTDatabase() const;

    //! Create rooted databases using names and rootes specified in the yt-config by user.
    std::vector<DB::DatabasePtr> CreateUserDefinedDatabases() const;

    std::vector<TString> GetUserDefinedDatabaseNames() const;

    void SetContext(DB::ContextMutablePtr context);
    DB::ContextMutablePtr GetContext() const;

    void InitQueryRegistry();
    void InitSingletones();

    NTableClient::TTableColumnarStatisticsCachePtr GetTableColumnarStatisticsCache() const;

    bool HasUserDefinedSqlObjectStorage() const;
    IUserDefinedSqlObjectsYTStorage* GetUserDefinedSqlObjectStorage();

    void SetSqlObjectOnOtherInstances(const TString& objectName, const TSqlObjectInfo& info) const;
    void RemoveSqlObjectOnOtherInstances(const TString& objectName, NHydra::TRevision revision) const;

    void ReloadDictionaryGlobally(const std::string& dictionaryName) const;

    TCypressDictionaryConfigRepositoryPtr GetCypressDictionaryConfigRepository();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(THost)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
