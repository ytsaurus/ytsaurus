#pragma once

#include "cluster_nodes.h"
#include "private.h"
#include "config.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/ytree/permission.h>

#include <string>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class THost
    : public TRefCounted
{
public:
    THost(
        IInvokerPtr controlInvoker,
        TPorts ports,
        TYtConfigPtr config,
        NApi::NNative::TConnectionConfigPtr connectionConfig);

    virtual ~THost() override;

    void Start();

    void HandleIncomingGossip(const TString& instanceId, EInstanceState state);

    TFuture<void> StopDiscovery();

    void ValidateReadPermissions(const std::vector<NYPath::TRichYPath>& paths, const TString& user);

    std::vector<TErrorOr<NYTree::IAttributeDictionaryPtr>> GetObjectAttributes(
        const std::vector<NYPath::TYPath>& paths,
        const NApi::NNative::IClientPtr& client);

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
    NApi::NNative::IClientPtr CreateClient(const TString& user);

    TClusterNodes GetNodes() const;

    int GetInstanceCookie() const;

    const NChunkClient::IMultiReaderMemoryManagerPtr& GetMultiReaderMemoryManager() const;

    const TQueryStatisticsReporterPtr& GetQueryStatisticsReporter() const;

    TYtConfigPtr GetConfig() const;

    EInstanceState GetInstanceState() const;

    void HandleCrashSignal() const;
    void HandleSigint();

    TQueryRegistryPtr GetQueryRegistry() const;

    //! Return future which is set when no query is executing.
    TFuture<void> GetIdleFuture() const;

    void SaveQueryRegistryState();

    void PopulateSystemDatabase(DB::IDatabase* systemDatabase) const;
    std::shared_ptr<DB::IDatabase> CreateYtDatabase() const;
    void SetContext(DB::ContextPtr context);

    NTableClient::TTableColumnarStatisticsCachePtr GetTableColumnarStatisticsCache() const;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(THost)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
