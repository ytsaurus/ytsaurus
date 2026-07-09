#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/flow/library/cpp/common/public.h>

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

// State is stored as is in column "state".
// Compressed is delta-coded and stored compressed in columns "compressed" and "compressed_patch".

struct TInternalState
    : public NYTree::TYsonStruct
{
    std::optional<NYson::TYsonString> State;
    std::optional<NYson::TYsonString> Compressed;

    REGISTER_YSON_STRUCT(TInternalState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TInternalState);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
