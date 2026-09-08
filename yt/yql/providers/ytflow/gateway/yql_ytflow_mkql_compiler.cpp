#include "yql_ytflow_mkql_compiler.h"

#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/providers/common/mkql/yql_provider_mkql.h>

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>
#include <yt/yql/providers/ytflow/integration/interface/yql_ytflow_integration.h>
#include <yt/yql/providers/ytflow/provider/yql_ytflow_configuration.h>
#include <yt/yql/providers/ytflow/provider/yql_ytflow_utils.h>

#include <util/string/cast.h>

namespace NYql {

using namespace NNodes;
using namespace NKikimr::NMiniKQL;
using namespace NUdf;

namespace {

template <typename TValue>
TRuntimeNode MkqlBuildDataLiteral(
    TValue&& value,
    NCommon::TMkqlBuildContext& buildCtx
) {
    return buildCtx.ProgramBuilder.NewDataLiteral(
        std::forward<TValue>(value));
}

template <typename TValue>
TRuntimeNode MkqlBuildStringLiteral(
    TValue&& value,
    NCommon::TMkqlBuildContext& buildCtx
) {
    return buildCtx.ProgramBuilder.NewDataLiteral<
        EDataSlot::String>(std::forward<TValue>(value));
}

TRuntimeNode MkqlBuildStringAtom(
    const TExprNode& node,
    NCommon::TMkqlBuildContext& buildCtx
) {
    auto atom = TCoAtom(&node);
    return MkqlBuildStringLiteral(atom.Value(), buildCtx);
}

TRuntimeNode MkqlBuildTupleOfStringAtoms(
    const TExprNode& node,
    NCommon::TMkqlBuildContext& buildCtx
) {
    TVector<TRuntimeNode> values;
    values.reserve(node.ChildrenSize());

    for (const auto& child : node.Children()) {
        values.push_back(MkqlBuildStringAtom(*child, buildCtx));
    }

    return buildCtx.ProgramBuilder.NewTuple(std::move(values));
}

} // anonymous namespace

void RegisterYtflowMkqlCompiler(
    NCommon::TMkqlCallableCompilerBase& compiler,
    const TTypeAnnotationContext& ctx,
    const TYtflowSettings& config
) {
    compiler.AddCallable(TYtflowLookupJoin::CallableName(),
        [&ctx, &config](const TExprNode& node, NCommon::TMkqlBuildContext& buildCtx) {
            auto lookupJoin = TYtflowLookupJoin(&node);

            auto outputType = buildCtx.BuildType(
                lookupJoin.Ref(),
                *lookupJoin.Ref().GetTypeAnn());

            auto stream = MkqlBuildExpr(lookupJoin.Stream().Ref(), buildCtx);

            auto lookupSourceType = buildCtx.BuildType(
                lookupJoin.LookupSource().Ref(),
                *lookupJoin.LookupSource().Ref().GetTypeAnn());

            auto wrappedLookupSourceType = buildCtx.ProgramBuilder.Nop(
                buildCtx.ProgramBuilder.NewVoid(), lookupSourceType);

            auto maybeProviderRead = lookupJoin
                .LookupSource().Maybe<TYtflowReadWrap>().Input();

            YQL_ENSURE(maybeProviderRead);

            auto [providerName, ytflowIntegration] = GetYtflowIntegrationWithProviderName(
                maybeProviderRead.Cast().Ref(), ctx);

            YQL_ENSURE(ytflowIntegration);

            auto provider = MkqlBuildStringLiteral(providerName, buildCtx);

            auto providerLookupSourceArgs = ytflowIntegration->BuildLookupSourceArgs(
                maybeProviderRead.Cast().Ref(), buildCtx);

            auto lookupSourceArgs = buildCtx.ProgramBuilder.NewTuple(TVector<TRuntimeNode>{
                std::move(provider), std::move(providerLookupSourceArgs)
            });

            auto joinKindEnum = NCommon::GetJoinKind(
                node, TCoAtom(lookupJoin.JoinKind().Raw()).Value());

            auto joinKind = MkqlBuildDataLiteral(
                static_cast<ui32>(joinKindEnum), buildCtx);

            auto buildScope = [&buildCtx](const auto& scope) {
                auto label = MkqlBuildStringAtom(scope.Label().Ref(), buildCtx);
                auto side = MkqlBuildStringAtom(scope.Side().Ref(), buildCtx);
                auto keys = MkqlBuildTupleOfStringAtoms(scope.Keys().Ref(), buildCtx);

                auto rowSelectionModeEnum = FromString<ERowSelectionMode>(
                    TCoAtom(scope.RowSelectionMode().Raw()).Value());

                auto rowSelectionMode = MkqlBuildDataLiteral(
                    static_cast<ui32>(rowSelectionModeEnum), buildCtx);

                return buildCtx.ProgramBuilder.NewTuple(TVector<TRuntimeNode>{
                    std::move(label),
                    std::move(side),
                    std::move(keys),
                    std::move(rowSelectionMode)
                });
            };

            auto streamScope = buildScope(lookupJoin.StreamScope());
            auto lookupSourceScope = buildScope(lookupJoin.LookupSourceScope());

            auto lookupJoinInflightRowLimit = config.LookupJoinInflightRowLimit.Get();
            YQL_ENSURE(
                lookupJoinInflightRowLimit,
                "Ytflow.LookupJoinInflightRowLimit pragma is not set");

            auto lookupJoinInflightLookupLimit = config.LookupJoinInflightLookupLimit.Get();
            YQL_ENSURE(
                lookupJoinInflightLookupLimit,
                "Ytflow.LookupJoinInflightLookupLimit pragma is not set");

            auto lookupJoinLookupTimeout = config.LookupJoinLookupTimeout.Get();
            YQL_ENSURE(
                lookupJoinLookupTimeout,
                "Ytflow.LookupJoinLookupTimeout pragma is not set");

            auto settings = buildCtx.ProgramBuilder.NewTuple(TVector<TRuntimeNode>{
                MkqlBuildDataLiteral(
                    static_cast<ui64>(*lookupJoinInflightRowLimit), buildCtx),
                MkqlBuildDataLiteral(
                    static_cast<ui64>(*lookupJoinInflightLookupLimit), buildCtx),
                MkqlBuildDataLiteral(lookupJoinLookupTimeout->MilliSeconds(), buildCtx),
            });

            TCallableBuilder call(
                buildCtx.ProgramBuilder.GetTypeEnvironment(),
                TYtflowLookupJoin::CallableName(),
                outputType);

            call.Add(stream);
            call.Add(wrappedLookupSourceType);
            call.Add(lookupSourceArgs);
            call.Add(joinKind);
            call.Add(streamScope);
            call.Add(lookupSourceScope);
            call.Add(settings);

            return TRuntimeNode(call.Build(), /*immediate*/ false);
        });

    compiler.AddCallable(TYtflowChunkedForwardList::CallableName(),
        [](const TExprNode& node, NCommon::TMkqlBuildContext& buildCtx) {
            auto chunkedForwardList = TYtflowChunkedForwardList(&node);

            auto outputType = buildCtx.BuildType(
                chunkedForwardList.Ref(),
                *chunkedForwardList.Ref().GetTypeAnn());

            auto stream = MkqlBuildExpr(chunkedForwardList.Stream().Ref(), buildCtx);

            TCallableBuilder call(
                buildCtx.ProgramBuilder.GetTypeEnvironment(),
                TYtflowChunkedForwardList::CallableName(),
                outputType);

            call.Add(stream);

            return TRuntimeNode(call.Build(), false);
        });
}

} // namespace NYql
