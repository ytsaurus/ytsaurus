#pragma once

#include "public.h"

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TCypressRegistrarOptions
{
    //! Path to the map node to be created and updated.
    NYPath::TYPath RootPath;

    //! Addresses for the nested orchid node. If |nullopt|, orchid will not be created.
    std::optional<NNodeTrackerClient::TAddressMap> OrchidRemoteAddresses;

    //! If true, the root node will be auto-deleted when registrar dies.
    bool ExpireSelf = false;

    //! If true, the registrar will create |alive| child with corresponding |expiration_time|.
    bool CreateAliveChild = false;

    //! If true, the registrar will try to create nodes upon first call to |UpdateNodes|.
    //! Otherwise it is up to caller to call |CreateNodes|.
    bool EnableImplicitInitialization = true;

    //! Attributes that should be set when the root node is created. They are not set
    //! if the node already exists.
    NYTree::IAttributeDictionaryPtr AttributesOnCreation;

    //! Attributes that should be set upon |CreateNodes| call.
    NYTree::IAttributeDictionaryPtr AttributesOnStart;

    //! Additional options for all Cypress requests.
    bool SuppressUpstreamSync = true;
    bool SuppressTransactionCoordinatorSync = true;

    //! Some components (e.g. rpc- and http-proxies) have special Cypress node type.
    NObjectClient::EObjectType NodeType = NObjectClient::EObjectType::MapNode;
};

////////////////////////////////////////////////////////////////////////////////

class TCypressRegistrarConfig
    : public NYTree::TYsonStruct
{
public:
    //! Period to call |UpdateNodes| when the registrar is run in periodic mode.
    TDuration UpdatePeriod;

    std::optional<TDuration> RootNodeTtl;
    std::optional<TDuration> AliveChildTtl;

    //! Timeout for all Cypress requests.
    TDuration RequestTimeout;

    REGISTER_YSON_STRUCT(TCypressRegistrarConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressRegistrarConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
