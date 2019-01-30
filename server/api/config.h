#pragma once

#include "public.h"

#include <yp/server/objects/public.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYP::NServer::NApi {

////////////////////////////////////////////////////////////////////////////////

class TGetUserAccessAllowedToConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    std::vector<NObjects::EObjectType> AllowedObjectTypes;

    TGetUserAccessAllowedToConfig()
    {
        RegisterParameter("allowed_object_types", AllowedObjectTypes)
            .Default({NObjects::EObjectType::NetworkProject});
    }
};

DEFINE_REFCOUNTED_TYPE(TGetUserAccessAllowedToConfig)

////////////////////////////////////////////////////////////////////////////////

class TObjectServiceConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    TGetUserAccessAllowedToConfigPtr GetUserAccessAllowedTo;

    TObjectServiceConfig()
    {
        RegisterParameter("get_user_access_allowed_to", GetUserAccessAllowedTo)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TObjectServiceConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NApi
