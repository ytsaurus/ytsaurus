#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

struct TCypressKeyReaderConfig
    : public NYTree::TYsonStruct
{
    //! Prefix path for public keys (will be read from <Path>/<OwnerId>/<KeyId>).
    NYPath::TYPath Path;

    NApi::TSerializableMasterReadOptionsPtr CypressReadOptions;

    REGISTER_YSON_STRUCT(TCypressKeyReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressKeyReaderConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSignatureValidationConfig
    : public NYTree::TYsonStruct
{
    TCypressKeyReaderConfigPtr CypressKeyReader;

    REGISTER_YSON_STRUCT(TSignatureValidationConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSignatureValidationConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
