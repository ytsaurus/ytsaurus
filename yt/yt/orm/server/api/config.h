#pragma once

#include "public.h"

#include <yt/yt/core/rpc/config.h>

namespace NYT::NOrm::NServer::NApi {

////////////////////////////////////////////////////////////////////////////////

class TObjectServiceConfig
    : public NRpc::TServiceConfig
{
public:
    NYson::EUnknownYsonFieldsMode ProtobufFormatUnknownFieldsMode;
    bool EnforceReadPermissions;
    bool EnableRequestCancelation;
    bool EnableMutatingRequestCancelation;

    REGISTER_YSON_STRUCT(TObjectServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TObjectServiceConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NApi
