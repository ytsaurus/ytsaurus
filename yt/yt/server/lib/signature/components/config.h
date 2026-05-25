#pragma once

#include "public.h"

#include <yt/yt/server/lib/signature/generation/public.h>

#include <yt/yt/server/lib/signature/validation/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

struct TSignatureComponentsConfig
    : public NYTree::TYsonStruct
{
    TSignatureValidationConfigPtr Validation;
    TSignatureGenerationConfigPtr Generation;

    // COMPAT(pavook): Default to false in 26.1, remove in 26.2.
    //! Whether we should use root user or a separate keysmith user (which got added in 25.4).
    bool UseRootUser;

    REGISTER_YSON_STRUCT(TSignatureComponentsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSignatureComponentsConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
