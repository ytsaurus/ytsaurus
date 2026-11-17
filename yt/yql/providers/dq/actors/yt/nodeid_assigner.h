#pragma once

#include <yt/yql/providers/dq/common/attrs/attrs.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NYql {

struct TAssignNodeIdOptions {
    TString ProxyAddress;
    TString User;
    TString Token;
    TString Prefix;
    TString Role;
    TString NodeName;

    THashMap<TString, TString> Attributes;

    ui32 MinNodeId = 0;
    ui32 MaxNodeId = 1<<18;

    TMaybe<ui32> NodeId; // for debug only
};

ui32 AssignNodeId(const TAssignNodeIdOptions& options);

} // namespace NYql
