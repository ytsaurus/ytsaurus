#pragma once

#include "public.h"

#include <yp/server/objects/public.h>

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/query_client/public.h>

#include <yt/core/ypath/public.h>

#include <yt/core/actions/signal.h>

namespace NYP {
namespace NServer {
namespace NMaster {

////////////////////////////////////////////////////////////////////////////////

struct TMasterDiscoveryInfo
{
    TString Fqdn;
    TString ClientGrpcAddress;
    TString SecureClientGrpcAddress;
    TString ClientHttpAddress;
    TString SecureClientHttpAddress;
    TString AgentGrpcAddress;
    TMasterInstanceTag InstanceTag = 0;
    bool Alive = false;
    bool Leading = false;
};

class TYTConnector
    : public TRefCounted
{
public:
    TYTConnector(TBootstrap* bootstrap, TYTConnectorConfigPtr config);

    void Initialize();

    const NYT::NApi::INativeClientPtr& GetClient();
    const NYT::NQueryClient::TTypeInferrerMapPtr& GetTypeInferrers();
    const NYT::NQueryClient::TFunctionProfilerMapPtr& GetFunctionProfilers();
    const NYT::NYPath::TYPath& GetRootPath();
    const NYT::NYPath::TYPath& GetDBPath();
    NYT::NYPath::TYPath GetTablePath(const NObjects::TDBTable* table);
    TClusterTag GetClusterTag();
    TMasterInstanceTag GetInstanceTag();

    bool IsConnected();
    bool IsLeading();

    const NYT::NApi::ITransactionPtr& GetInstanceLockTransaction();

    //! Returns the (periodically updatable) list of known masters.
    /*!
     *  Thread affinity: any
     */
    std::vector<TMasterDiscoveryInfo> GetMasters();

    DECLARE_SIGNAL(void(), ValidateConnection);
    DECLARE_SIGNAL(void(), Connected);
    DECLARE_SIGNAL(void(), Disconnected);

    DECLARE_SIGNAL(void(), StartedLeading);
    DECLARE_SIGNAL(void(), StoppedLeading);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TYTConnector)

////////////////////////////////////////////////////////////////////////////////

} // namespace NMaster
} // namespace NServer
} // namespace NYP
