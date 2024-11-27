#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

struct TSignatureGeneratorConfig
    : public NYTree::TYsonStruct
{
    //! Delta between key creation and expiration.
    TDuration KeyExpirationDelta;

    //! Delta between signature creation and expiration.
    TDuration SignatureExpirationDelta;

    //! Margin of time synchronization error. ValidAfter is set to creation time minus this margin.
    TDuration TimeSyncMargin;

    REGISTER_YSON_STRUCT(TSignatureGeneratorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSignatureGeneratorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSignatureValidatorConfig
    : public NYTree::TYsonStruct
{
    REGISTER_YSON_STRUCT(TSignatureValidatorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSignatureValidatorConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
