#pragma once

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/providers/common/mkql/yql_provider_mkql.h>

#include <yt/yql/providers/yt/lib/lambda_builder/lambda_builder.h>

#include <util/generic/string.h>


namespace NYql::NYtflow {

struct TLambdaArgument
{
    TString Name;
    TMaybe<ETypeAnnotationKind> Kind;
};

class TYtflowLambdaBuilder: public TLambdaBuilder
{
public:
    using TLambdaBuilder::TLambdaBuilder;

public:
    TString BuildLambdaWithIO(
        const NYql::NCommon::IMkqlCallableCompiler& compiler,
        NYql::NNodes::TCoLambda lambda,
        const TVector<TLambdaArgument>& lambdaArguments,
        NYql::TExprContext& exprCtx,
        NYql::TLangVersion langVer,
        NYql::TRuntimeSettings::TConstPtr runtimeSettings);
};

} // namespace NYql::NYtflow
