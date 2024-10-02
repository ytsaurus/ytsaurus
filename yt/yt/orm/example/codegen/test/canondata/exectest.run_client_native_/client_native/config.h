// AUTOMATICALLY GENERATED. DO NOT EDIT!

#pragma once

#include "public.h"

#include <yt/yt/orm/client/native/config.h>

namespace NYT::NOrm::NExample::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

class TConnectionConfig
    : public NYT::NOrm::NClient::NNative::TConnectionConfig
{
public:
    REGISTER_YSON_STRUCT(TConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NClient::NNative
