#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>


namespace NYql {

class IYtflowLookupProviderRegistry;

} // namespace NYql

namespace NYql::NYtflow {

enum class EYtflowCallablePatternSharing {
    Unknown,
    Shareable,
    PrivateOnly,
};

struct TYtflowPatternMetadata {
    THashMap<TString, EYtflowCallablePatternSharing> SpecializedCallables;
};

struct TNodeFactoryMetadata {
    const IYtflowLookupProviderRegistry& YtflowLookupProviderRegistry;
};

struct TNodeFactoryResult {
    THashMap<TString, const NKikimr::NMiniKQL::TType*> InputTypes;
    THashMap<TString, NKikimr::NMiniKQL::IComputationExternalNode*> YtflowInputNodes;
    TYtflowPatternMetadata PatternMetadata;
};

NKikimr::NMiniKQL::TComputationNodeFactory GetNodeFactory(
    const TNodeFactoryMetadata& metadata,
    TNodeFactoryResult& result);

} // namespace NYql::NYtflow
