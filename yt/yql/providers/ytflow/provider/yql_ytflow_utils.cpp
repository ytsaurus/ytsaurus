#include "yql_ytflow_utils.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/expr_nodes_gen/yql_expr_nodes_gen.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>
#include <yt/yql/providers/ytflow/integration/interface/yql_ytflow_integration.h>
#include <yt/yql/providers/ytflow/integration/interface/yql_ytflow_optimization.h>
#include <yt/yql/providers/ytflow/integration/proto/yt.pb.h>

#include <util/generic/hash_set.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>

#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/join.h>

#include <google/protobuf/any.pb.h>


namespace NYql {

using namespace NNodes;


IDataProvider* GetDataProvider(
    const TExprNode& node,
    const TTypeAnnotationContext& typeCtx
) {
    if (node.ChildrenSize() > 1) {
        if (auto maybeDataSource = TMaybeNode<TCoDataSource>(node.Child(1))) {
            auto dataSourceName = maybeDataSource.Cast().Category();
            if (auto dataSource = typeCtx.DataSourceMap.FindPtr(dataSourceName)) {
                return dataSource->Get();
            }
        } else if (auto maybeDataSink = TMaybeNode<TCoDataSink>(node.Child(1))) {
            auto dataSinkName = maybeDataSink.Cast().Category();
            if (auto dataSink = typeCtx.DataSinkMap.FindPtr(dataSinkName)) {
                return dataSink->Get();
            }
        }
    }

    return nullptr;
}

std::pair<TString, IYtflowIntegration*> GetYtflowIntegrationWithProviderName(
    const TExprNode& node,
    const TTypeAnnotationContext& typeCtx
) {
    auto* dataProvider = GetDataProvider(node, typeCtx);
    if (dataProvider) {
        return {TString(dataProvider->GetName()), dataProvider->GetYtflowIntegration()};
    }

    return {"", nullptr};
}

IYtflowIntegration* GetYtflowIntegration(
    const TExprNode& node,
    const TTypeAnnotationContext& typeCtx
) {
    return GetYtflowIntegrationWithProviderName(node, typeCtx).second;
}

IYtflowOptimization* GetYtflowOptimization(
    const TExprNode& node,
    const TTypeAnnotationContext& typeCtx
) {
    auto* dataProvider = GetDataProvider(node, typeCtx);
    if (dataProvider) {
        return dataProvider->GetYtflowOptimization();
    }

    return nullptr;
}

bool EnsureSpecificCallable(
    const TExprNode& node,
    const THashSet<TStringBuf>& callableNames,
    TExprContext& ctx
) {
    if (!EnsureCallable(node, ctx)) {
        return false;
    }

    if (!node.IsCallable(callableNames)) {
        ctx.AddError(TIssue(
            ctx.GetPosition(node.Pos()),
            TStringBuilder()
                << "Expected callables: " << JoinSeq(", ", callableNames)
                << ", but got: " << node.Content()));

        return false;
    }

    return true;
}

bool EnsureSpecificDataSource(
    const TExprNode& node,
    const THashSet<TStringBuf>& expectedCategories,
    TExprContext& ctx
) {
    if (!EnsureDataSource(node, ctx)) {
        return false;
    }

    auto category = TCoDataSource(&node).Category().Value();

    if (!expectedCategories.contains(category)) {
        ctx.AddError(TIssue(
            ctx.GetPosition(node.Pos()),
            TStringBuilder()
                << "Expected datasource category: " << JoinSeq(", ", expectedCategories)
                << ", but got: " << category));

        return false;
    }

    return true;
}

bool EnsureSpecificDataSink(
    const TExprNode& node,
    const THashSet<TStringBuf>& expectedCategories,
    TExprContext& ctx
) {
    if (!EnsureDataSink(node, ctx)) {
        return false;
    }

    auto category = TCoDataSink(&node).Category().Value();

    if (!expectedCategories.contains(category)) {
        ctx.AddError(TIssue(
            ctx.GetPosition(node.Pos()),
            TStringBuilder()
                << "Expected datasink category: " << JoinSeq(", ", expectedCategories)
                << ", but got: " << category));

        return false;
    }

    return true;
}

bool IsYtPersistentSink(
    const TExprNode& node,
    TExprContext& ctx,
    const TTypeAnnotationContext& typeCtx
) {
    NYtflow::NProto::TQYTSinkMessage sinkSettings;
    return TryGetYtSinkSettings(node, ctx, typeCtx, sinkSettings);
}

bool TryGetYtSinkSettings(
    const TExprNode& node,
    TExprContext& ctx,
    const TTypeAnnotationContext& typeCtx,
    NYtflow::NProto::TQYTSinkMessage& sinkSettings
) {
    auto maybePersistentSink = TMaybeNode<TYtflowPersistentSink>(&node);
    if (!maybePersistentSink) {
        return false;
    }

    auto maybeWriteWrap = maybePersistentSink.Input().Maybe<TYtflowWriteWrap>();
    if (!maybeWriteWrap) {
        return false;
    }

    auto input = maybeWriteWrap.Cast().Input();

    auto* ytflowIntegration = GetYtflowIntegration(input.Ref(), typeCtx);
    YQL_ENSURE(ytflowIntegration);

    ::google::protobuf::Any settings;
    ytflowIntegration->FillSinkSettings(input.Ref(), settings, ctx);

    return settings.UnpackTo(&sinkSettings);
}

bool IsYtflowProviderInput(const TExprNode& node) {
    return TYtflowReadWrap::Match(&node) || TYtflowOutput::Match(&node);
}

TExprNode::TPtr BuildOperationSource(
    const TExprNode::TPtr& input,
    TSyncMap& syncList,
    TExprContext& ctx,
    const TTypeAnnotationContext& typeCtx
) {
    TExprNode::TPtr source;
    TExprNode::TPtr world;

    if (auto maybeReadWrap = TMaybeNode<TYtflowReadWrap>(input)) {
        auto providerInput = maybeReadWrap.Cast().Input();

        auto* ytflowIntegration = GetYtflowIntegration(providerInput.Ref(), typeCtx);
        YQL_ENSURE(ytflowIntegration);

        source = Build<TYtflowPersistentSource>(ctx, maybeReadWrap.Cast().Pos())
            .Name()
                .Value("")
                .Build()
            .Input(maybeReadWrap.Cast())
            .Done().Ptr();

        world = ytflowIntegration->GetReadWorld(providerInput.Ref(), ctx);
    } else {
        auto output = TYtflowOutput(input);
        source = output.Ptr();
        world = output.Operation().World().Ptr();
    }

    syncList.emplace(world, syncList.size());

    return source;
}

const TStructExprType* FilterMembers(
    const TStructExprType* structType,
    const TVector<TStringBuf>& members,
    TExprContext& ctx
) {
    const auto& items = structType->GetItems();
    TVector<const TItemExprType*> extractedItems;

    for (const auto& member : members) {
        auto maybeIndex = structType->FindItem(member);
        YQL_ENSURE(maybeIndex);
        extractedItems.push_back(items[*maybeIndex]);
    }

    return ctx.MakeType<TStructExprType>(std::move(extractedItems));
}

TVector<TString> ParseTupleOfAtoms(const TExprNode& node) {
    TVector<TString> values;
    values.reserve(node.ChildrenSize());

    for (const auto& child : node.Children()) {
        values.push_back(TString(child->Content()));
    }

    return values;
}

bool IsTrivialLambda(const TExprNode& node) {
    YQL_ENSURE(node.IsLambda());

    if (node.Head().ChildrenSize() != 1) {
        return false;
    }

    return &node.Head().Head() == &node.Tail();
}

} // namespace NYql
