#include "yql_ytflow_secure_params.h"

#include <yql/essentials/minikql/computation/mkql_computation_node.h>

#include <yt/yql/providers/ytflow/common/yql_ytflow_environment.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/system/env.h>

#include <utility>

namespace NYql::NYtflow {

std::unique_ptr<NUdf::ISecureParamsProvider> CreateSecureParamsProvider()
{
    auto secureParamsEnv = GetEnv(SecureParamsEnvironmentVariable);
    if (!secureParamsEnv) {
        return nullptr;
    }

    auto secureParams = NYT::NYTree::ConvertTo<THashMap<TString, TString>>(
        NYT::NYson::TYsonString(std::move(secureParamsEnv)));
    return NKikimr::NMiniKQL::MakeSimpleSecureParamsProvider(std::move(secureParams));
}

} // namespace NYql::NYtflow
