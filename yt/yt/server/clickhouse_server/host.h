#pragma once

#include "cluster_nodes.h"
#include "private.h"
#include "config.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/core/actions/public.h>

#include <yt/core/ytree/permission.h>

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

    std::vector<TErrorOr<NYTree::TAttributeMap>> GetObjectAttributes(
        const std::vector<NYPath::TYPath>& paths,
        const NApi::NNative::IClientPtr& client);

    const IInvokerPtr& GetControlInvoker() const;
    const IInvokerPtr& GetWorkerInvoker() const;

    NApi::NNative::IClientPtr GetRootClient() const;
    NApi::NNative::IClientPtr CreateClient(TString user);

    TClusterNodes GetNodes() const;

    const NChunkClient::IMultiReaderMemoryManagerPtr& GetMultiReaderMemoryManager() const;

    TYtConfigPtr GetConfig() const;

    EInstanceState GetInstanceState() const;

    void HandleCrashSignal() const;
    void HandleSigint();

    TQueryRegistryPtr GetQueryRegistry() const;

    //! Return future which is set when no query is executing.
    TFuture<void> GetIdleFuture() const;

    void SaveQueryRegistryState();

    std::unique_ptr<DB::IUsersManager> CreateUsersManager() const;
    void PopulateSystemDatabase(DB::IDatabase* systemDatabase) const;
    std::shared_ptr<DB::IDatabase> CreateYtDatabase() const;
    void SetContext(DB::Context* context);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(THost)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
