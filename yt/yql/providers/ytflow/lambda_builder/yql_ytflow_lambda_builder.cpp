#include "yql_ytflow_lambda_builder.h"

#include <library/cpp/iterator/zip.h>

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/providers/common/mkql/yql_provider_mkql.h>
#include <yql/essentials/providers/common/mkql/yql_type_mkql.h>
#include <yql/essentials/utils/yql_panic.h>


namespace NYql::NYtflow {

TString TYtflowLambdaBuilder::BuildLambdaWithIO(
    const NYql::NCommon::IMkqlCallableCompiler& compiler,
    NYql::NNodes::TCoLambda lambda,
    const TVector<TLambdaArgument>& lambdaArguments,
    NYql::TExprContext& exprCtx,
    NYql::TLangVersion langVer,
    NYql::TRuntimeSettings::TConstPtr runtimeSettings)
{
    NKikimr::NMiniKQL::TProgramBuilder pgmBuilder(
        GetTypeEnvironment(), GetFunctionRegistry(),
        /*voidWithEffects*/ false, langVer, runtimeSettings);

    YQL_ENSURE(
        lambdaArguments.size() >= 1,
        "Expected lambda with some args");

    YQL_ENSURE(
        lambda.Args().Size() == lambdaArguments.size(),
        "Argument count mismatch: "
            << lambda.Args().Size() << " (node) != "
            << lambdaArguments.size() << " (expected)");

    TArgumentsMap arguments(lambdaArguments.size());

    for (const auto& [arg, lambdaArgument] : Zip(lambda.Args(), lambdaArguments)) {
        const auto argType = arg.Ref().GetTypeAnn();

        if (lambdaArgument.Kind.Defined()) {
            YQL_ENSURE(
                argType->GetKind() == lambdaArgument.Kind.GetRef(),
                "Argument " << lambdaArgument.Name << " type mismatch: "
                    << argType->GetKind() << " (node) != "
                    << lambdaArgument.Kind.GetRef() << " (expected)");
        }

        auto inputType = NCommon::BuildType(arg.Ref(), *argType, pgmBuilder);

        NKikimr::NMiniKQL::TCallableBuilder inputCall(
            GetTypeEnvironment(), lambdaArgument.Name, inputType);

        auto inputStream = NKikimr::NMiniKQL::TRuntimeNode(inputCall.Build(), false);

        arguments[arg.Raw()] = inputStream;
    }

    NCommon::TMkqlBuildContext buildCtx(
        compiler, pgmBuilder, exprCtx, lambda.Ref().UniqueId(), std::move(arguments));

    auto rootNode = NCommon::MkqlBuildExpr(lambda.Body().Ref(), buildCtx);
    auto [serializedNode, nodeCount] = Serialize(std::move(rootNode));

    return serializedNode;
}

} // namespace NYql::NYtflow
