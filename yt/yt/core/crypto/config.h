#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NCrypto {

////////////////////////////////////////////////////////////////////////////////

//! Either an inlined value or a file reference.
class TPemBlobConfig
    : public NYTree::TYsonSerializable
{
public:
    std::optional<TString> FileName;
    std::optional<TString> Value;

    TPemBlobConfig();

    TString LoadBlob() const;
};

DEFINE_REFCOUNTED_TYPE(TPemBlobConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrypto
