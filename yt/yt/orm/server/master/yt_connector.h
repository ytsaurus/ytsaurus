#pragma once

#include "public.h"

#include <yt/yt/orm/server/objects/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/library/query/base/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/actions/signal.h>
#include <yt/yt/core/ypath/public.h>
#include <yt/yt/core/ytree/public.h>

namespace NYT::NOrm::NServer::NMaster {

////////////////////////////////////////////////////////////////////////////////

struct TMasterDiscoveryInfo
{
    TString Fqdn;
    TString IP6Address;
    TString ClientGrpcAddress;
    TString ClientGrpcIP6Address;
    TString SecureClientGrpcAddress;
    TString SecureClientGrpcIP6Address;
    TString ClientHttpAddress;
    TString ClientHttpIP6Address;
    TString SecureClientHttpAddress;
    TString SecureClientHttpIP6Address;
    TString RpcProxyAddress;
    TString RpcProxyIP6Address;
    TMasterInstanceTag InstanceTag = UndefinedMasterInstanceTag;
    TInstant LockTime = TInstant::Zero();
    bool Alive = false;
    bool Leading = false;
    NYTree::IAttributeDictionaryPtr DynamicAttributes;
};

using TMasterDiscoveryInfoValidator = std::function<void(const TMasterDiscoveryInfo&)>;

////////////////////////////////////////////////////////////////////////////////

class TYTConnector
    : public TRefCounted
{
public:
    TYTConnector(
        IBootstrap* bootstrap,
        TYTConnectorConfigPtr config,
        int expectedDBVersion,
        std::vector<TString> dynamicAttributeKeys = {},
        TMasterDiscoveryInfoValidator masterDiscoveryInfoValidator = {});
    ~TYTConnector();

    void Initialize();

    //! Start is separated from the initialization to give
    //! time for all components to subscribe to signals.
    void Start();

    //! YT client for performing service commands for the control plane.
    //! Shall be used only for control structures (not data).
    NYT::NApi::IClientPtr GetControlClient();

    std::string FormatUserTag(std::string userTag = {}) const;
    //! Client requests are sent on behalf of ORM user (see #YTConnectorConfig)
    //! and tagged either by #userTag (if provided) or RPC user of the current fiber.
    NYT::NApi::IClientPtr GetClient(std::string identityUserTag);
    //! Aborts if native client is not supported by the configuration.
    NYT::NApi::NNative::IClientPtr GetNativeClient(std::string identityUserTag);
    //! Aborts if native connection is not supported by the configuration.
    NYT::NApi::NNative::IConnectionPtr GetNativeConnection();
    const NYPath::TYPath& GetRootPath();
    const NYPath::TYPath& GetDBPath();
    const NYPath::TYPath& GetMasterPath();
    NYPath::TYPath GetConsumerPath(std::string_view name);
    NYPath::TYPath GetTablePath(const NObjects::TDBTable* table);
    TClusterTag GetClusterTag();
    const std::string& GetClusterName();
    TMasterInstanceTag GetInstanceTag();

    const TYTConnectorConfigPtr& GetConfig();

    bool IsConnected() const;
    TError CheckConnected() const;

    bool IsLeading() const;

    const NYT::NApi::ITransactionPtr& GetInstanceTransaction();

    int GetExpectedDBVersion() const;

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

} // namespace NYT::NOrm::NServer::NMaster
