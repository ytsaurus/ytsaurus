#include "dq_opt_join_cost_based.h"
#include "dq_opt_dphyp_solver.h"
#include "dq_opt_make_join_hypergraph.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_join.h>
#include <contrib/ydb/library/yql/dq/opt/dq_opt.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql::NDq {

using namespace NYql::NNodes;

/*
 * Collects EquiJoin inputs with statistics for cost based optimization
 */
bool DqCollectJoinRelationsWithStats(
    TVector<std::shared_ptr<TRelOptimizerNode>>& rels,
    TTypeAnnotationContext& typesCtx,
    const TCoEquiJoin& equiJoin,
    const TProviderCollectFunction& collector
) {
    if (equiJoin.ArgCount() < 3) {
        return false;
    }

    for (size_t i = 0; i < equiJoin.ArgCount() - 2; ++i) {
        auto input = equiJoin.Arg(i).Cast<TCoEquiJoinInput>();
        auto joinArg = input.List();

        auto stats = typesCtx.GetStats(joinArg.Raw());

        if (!stats) {
            YQL_CLOG(TRACE, CoreDq) << "Didn't find statistics for scope " << input.Scope().Cast<TCoAtom>().StringValue() << "\n";
            return false;
        }

        auto scope = input.Scope();
        if (!scope.Maybe<TCoAtom>()){
            return false;
        }

        TStringBuf label = scope.Cast<TCoAtom>();
        collector(rels, label, joinArg.Ptr(), stats);
    }
    return true;
}

/**
 * Convert JoinTuple from AST into an internal representation of a optimizer plan
 * This procedure also hooks up rels with statistics to the leaf nodes of the plan
 * Statistics for join nodes are not computed
*/
std::shared_ptr<TJoinOptimizerNode> ConvertToJoinTree(
    const TCoEquiJoinTuple& joinTuple,
    const TVector<std::shared_ptr<TRelOptimizerNode>>& rels
) {

    std::shared_ptr<IBaseOptimizerNode> left;
    std::shared_ptr<IBaseOptimizerNode> right;


    if (joinTuple.LeftScope().Maybe<TCoEquiJoinTuple>()) {
        left = ConvertToJoinTree(joinTuple.LeftScope().Cast<TCoEquiJoinTuple>(), rels);
    }
    else {
        auto scope = joinTuple.LeftScope().Cast<TCoAtom>().StringValue();
        auto it = find_if(rels.begin(), rels.end(), [scope] (const std::shared_ptr<TRelOptimizerNode>& n) {
            return scope == n->Label;
        } );
        left = *it;
    }

    if (joinTuple.RightScope().Maybe<TCoEquiJoinTuple>()) {
        right = ConvertToJoinTree(joinTuple.RightScope().Cast<TCoEquiJoinTuple>(), rels);
    }
    else {
        auto scope = joinTuple.RightScope().Cast<TCoAtom>().StringValue();
        auto it = find_if(rels.begin(), rels.end(), [scope] (const std::shared_ptr<TRelOptimizerNode>& n) {
            return scope == n->Label;
        } );
        right =  *it;
    }

    TVector<TJoinColumn> leftKeys;
    TVector<TJoinColumn> rightKeys;

    size_t joinKeysCount = joinTuple.LeftKeys().Size() / 2;
    for (size_t i = 0; i < joinKeysCount; ++i) {
        size_t keyIndex = i * 2;

        auto leftScope = joinTuple.LeftKeys().Item(keyIndex).StringValue();
        auto leftColumn = joinTuple.LeftKeys().Item(keyIndex + 1).StringValue();
        leftKeys.push_back(TJoinColumn(leftScope, leftColumn));

        auto rightScope = joinTuple.RightKeys().Item(keyIndex).StringValue();
        auto rightColumn = joinTuple.RightKeys().Item(keyIndex + 1).StringValue();
        rightKeys.push_back(TJoinColumn(rightScope, rightColumn));
    }

    const auto linkSettings = GetEquiJoinLinkSettings(joinTuple.Options().Ref());
    return std::make_shared<TJoinOptimizerNode>(left, right, leftKeys, rightKeys, ConvertToJoinKind(joinTuple.Type().StringValue()), EJoinAlgoType::Undefined,
        linkSettings.LeftHints.contains("any"), linkSettings.RightHints.contains("any"));
}

/**
 * Build a join tree that will replace the original join tree in equiJoin
 * TODO: Add join implementations here
*/
TExprBase BuildTree(TExprContext& ctx, const TCoEquiJoin& equiJoin,
    std::shared_ptr<TJoinOptimizerNode>& reorderResult) {

    // Create dummy left and right arg that will be overwritten
    TExprBase leftArg(equiJoin);
    TExprBase rightArg(equiJoin);

    // Build left argument of the join
    if (reorderResult->LeftArg->Kind == RelNodeType) {
        std::shared_ptr<TRelOptimizerNode> rel =
            std::static_pointer_cast<TRelOptimizerNode>(reorderResult->LeftArg);
        leftArg = BuildAtom(rel->Label, equiJoin.Pos(), ctx);
    } else {
        std::shared_ptr<TJoinOptimizerNode> join =
            std::static_pointer_cast<TJoinOptimizerNode>(reorderResult->LeftArg);
        leftArg = BuildTree(ctx,equiJoin,join);
    }
    // Build right argument of the join
    if (reorderResult->RightArg->Kind == RelNodeType) {
        std::shared_ptr<TRelOptimizerNode> rel =
            std::static_pointer_cast<TRelOptimizerNode>(reorderResult->RightArg);
        rightArg = BuildAtom(rel->Label, equiJoin.Pos(), ctx);
    } else {
        std::shared_ptr<TJoinOptimizerNode> join =
            std::static_pointer_cast<TJoinOptimizerNode>(reorderResult->RightArg);
        rightArg = BuildTree(ctx,equiJoin,join);
    }

    TVector<TExprBase> leftJoinColumns;
    TVector<TExprBase> rightJoinColumns;

    // Build join conditions
    for( auto leftKey : reorderResult->LeftJoinKeys) {
        leftJoinColumns.push_back(BuildAtom(leftKey.RelName, equiJoin.Pos(), ctx));
        leftJoinColumns.push_back(BuildAtom(leftKey.AttributeNameWithAliases, equiJoin.Pos(), ctx));
    }
    for( auto rightKey : reorderResult->RightJoinKeys) {
        rightJoinColumns.push_back(BuildAtom(rightKey.RelName, equiJoin.Pos(), ctx));
        rightJoinColumns.push_back(BuildAtom(rightKey.AttributeNameWithAliases, equiJoin.Pos(), ctx));
    }

    TExprNode::TListType options(1U,
        ctx.Builder(equiJoin.Pos())
            .List()
                .Atom(0, "join_algo", TNodeFlags::Default)
                .Atom(1, ToString(reorderResult->JoinAlgo), TNodeFlags::Default)
            .Seal()
        .Build()
    );

    if (reorderResult->LeftAny) {
        options.emplace_back(ctx.Builder(equiJoin.Pos())
            .List()
                .Atom(0, "left", TNodeFlags::Default)
                .Atom(1, "any", TNodeFlags::Default)
            .Seal()
        .Build());
    }

    if (reorderResult->RightAny) {
        options.emplace_back(ctx.Builder(equiJoin.Pos())
            .List()
                .Atom(0, "right", TNodeFlags::Default)
                .Atom(1, "any", TNodeFlags::Default)
            .Seal()
        .Build());
    }

    // Build the final output
    return Build<TCoEquiJoinTuple>(ctx,equiJoin.Pos())
        .Type(BuildAtom(ConvertToJoinString(reorderResult->JoinType),equiJoin.Pos(),ctx))
        .LeftScope(leftArg)
        .RightScope(rightArg)
        .LeftKeys()
            .Add(leftJoinColumns)
            .Build()
        .RightKeys()
            .Add(rightJoinColumns)
            .Build()
        .Options()
            .Add(std::move(options))
            .Build()
        .Done();
}

/**
 * Rebuild the equiJoinOperator with a new tree, that was obtained by optimizing join order
*/
TExprBase RearrangeEquiJoinTree(TExprContext& ctx, const TCoEquiJoin& equiJoin,
    std::shared_ptr<TJoinOptimizerNode> reorderResult) {
    TVector<TExprBase> joinArgs;
    for (size_t i = 0; i < equiJoin.ArgCount() - 2; i++){
        joinArgs.push_back(equiJoin.Arg(i));
    }

    joinArgs.push_back(BuildTree(ctx,equiJoin,reorderResult));

    joinArgs.push_back(equiJoin.Arg(equiJoin.ArgCount() - 1));

    return Build<TCoEquiJoin>(ctx, equiJoin.Pos())
        .Add(joinArgs)
        .Done();
}

/*
 * Recursively computes statistics for a join tree
 */
void ComputeStatistics(const std::shared_ptr<TJoinOptimizerNode>& join, IProviderContext& ctx) {
    if (join->LeftArg->Kind == EOptimizerNodeKind::JoinNodeType) {
        ComputeStatistics(static_pointer_cast<TJoinOptimizerNode>(join->LeftArg), ctx);
    }
    if (join->RightArg->Kind == EOptimizerNodeKind::JoinNodeType) {
        ComputeStatistics(static_pointer_cast<TJoinOptimizerNode>(join->RightArg), ctx);
    }
    join->Stats = TOptimizerStatistics(
        ctx.ComputeJoinStats(
            join->LeftArg->Stats, 
            join->RightArg->Stats,
            join->LeftJoinKeys, 
            join->RightJoinKeys, 
            EJoinAlgoType::GraceJoin,
            join->JoinType
        )
    );
}

class TOptimizerNativeNew: public IOptimizerNew {
public:
    TOptimizerNativeNew(IProviderContext& ctx, ui32 maxDPhypDPTableSize, TExprContext& exprCtx)
        : IOptimizerNew(ctx)
        , MaxDPHypTableSize_(maxDPhypDPTableSize)
        , ExprCtx(exprCtx)
    {}

    std::shared_ptr<TJoinOptimizerNode> JoinSearch(
        const std::shared_ptr<TJoinOptimizerNode>& joinTree, 
        const TOptimizerHints& hints = {}
    ) override {

        auto relsCount = joinTree->Labels().size();

        if (relsCount <= 64) { // The algorithm is more efficient.
            return JoinSearchImpl<TNodeSet64>(joinTree, hints);
        } else if (64 < relsCount && relsCount <= 128) {
            return JoinSearchImpl<TNodeSet128>(joinTree, hints);
        } else if (128 < relsCount && relsCount <= 192) {
            return JoinSearchImpl<TNodeSet192>(joinTree, hints);
        }

        ComputeStatistics(joinTree, this->Pctx);
        return joinTree;
    }

private:
    using TNodeSet64 = std::bitset<64>;
    using TNodeSet128 = std::bitset<128>;
    using TNodeSet192 = std::bitset<192>;

    template <typename TNodeSet>
    std::shared_ptr<TJoinOptimizerNode> JoinSearchImpl(
        const std::shared_ptr<TJoinOptimizerNode>& joinTree, 
        const TOptimizerHints& hints = {}
    ) {
        TJoinHypergraph<TNodeSet> hypergraph = MakeJoinHypergraph<TNodeSet>(joinTree, hints);
        TDPHypSolver<TNodeSet> solver(hypergraph, this->Pctx);

        if (solver.CountCC(MaxDPHypTableSize_) >= MaxDPHypTableSize_) {
            YQL_CLOG(TRACE, CoreDq) << "Maximum DPhyp threshold exceeded";
            ExprCtx.AddWarning(
                YqlIssue(
                    {}, TIssuesIds::CBO_ENUM_LIMIT_REACHED,
                    "Cost Based Optimizer could not be applied to this query: "
                    "Enumeration is too large, use PRAGMA MaxDPHypDPTableSize='4294967295' to disable the limitation"
                )
            );
            ComputeStatistics(joinTree, this->Pctx);
            return joinTree;
        }

        auto bestJoinOrder = solver.Solve(hints);
        auto resTree = ConvertFromInternal(bestJoinOrder);
        AddMissingConditions(hypergraph, resTree);
        return resTree;
    }

    /* Due to cycles we can miss some conditions in edges, because DPHyp enumerates trees */
    template <typename TNodeSet>
    void AddMissingConditions(
        TJoinHypergraph<TNodeSet>& hypergraph,
        const std::shared_ptr<IBaseOptimizerNode>& node
    ) {
        if (node->Kind != EOptimizerNodeKind::JoinNodeType) {
            return;
        }

        auto joinNode = std::static_pointer_cast<TJoinOptimizerNode>(node);
        AddMissingConditions(hypergraph, joinNode->LeftArg);
        AddMissingConditions(hypergraph, joinNode->RightArg);
        TNodeSet lhs = hypergraph.GetNodesByRelNames(joinNode->LeftArg->Labels());
        TNodeSet rhs = hypergraph.GetNodesByRelNames(joinNode->RightArg->Labels());

        hypergraph.FindAllConditionsBetween(lhs, rhs, joinNode->LeftJoinKeys, joinNode->RightJoinKeys);
    }

private:
    ui32 MaxDPHypTableSize_;
    TExprContext& ExprCtx;
};

IOptimizerNew* MakeNativeOptimizerNew(IProviderContext& pctx, const ui32 maxDPhypDPTableSize, TExprContext& ectx) {
    return new TOptimizerNativeNew(pctx, maxDPhypDPTableSize, ectx);
}

TExprBase DqOptimizeEquiJoinWithCosts(
    const TExprBase& node,
    TExprContext& ctx,
    TTypeAnnotationContext& typesCtx,
    ui32 optLevel,
    IOptimizerNew& opt,
    const TProviderCollectFunction& providerCollect,
    const TOptimizerHints& hints
) {
    int dummyEquiJoinCounter = 0;
    return DqOptimizeEquiJoinWithCosts(node, ctx, typesCtx, optLevel, opt, providerCollect, dummyEquiJoinCounter, hints);
}

TExprBase DqOptimizeEquiJoinWithCosts(
    const TExprBase& node,
    TExprContext& ctx,
    TTypeAnnotationContext& typesCtx,
    ui32 optLevel,
    IOptimizerNew& opt,
    const TProviderCollectFunction& providerCollect,
    int& equiJoinCounter,
    const TOptimizerHints& hints
) {
    if (optLevel <= 1) {
        return node;
    }

    if (!node.Maybe<TCoEquiJoin>()) {
        return node;
    }
    auto equiJoin = node.Cast<TCoEquiJoin>();
    YQL_ENSURE(equiJoin.ArgCount() >= 4);

    if (typesCtx.ContainsStats(equiJoin.Raw())) {
        return node;
    }

    YQL_CLOG(TRACE, CoreDq) << "Optimizing join with costs";

    TVector<std::shared_ptr<TRelOptimizerNode>> rels;

    // Check that statistics for all inputs of equiJoin were computed
    // The arguments of the EquiJoin are 1..n-2, n-2 is the actual join tree
    // of the EquiJoin and n-1 argument are the parameters to EquiJoin

    if (!DqCollectJoinRelationsWithStats(rels, typesCtx, equiJoin, providerCollect)){
        ctx.AddWarning(
            YqlIssue(ctx.GetPosition(equiJoin.Pos()), TIssuesIds::CBO_MISSING_TABLE_STATS,
            "Cost Based Optimizer could not be applied to this query: couldn't load statistics"
            )
        );
        return node;
    }

    YQL_CLOG(TRACE, CoreDq) << "All statistics for join in place";

    bool allRowStorage = std::all_of(
        rels.begin(), 
        rels.end(), 
        [](std::shared_ptr<TRelOptimizerNode>& r) {return r->Stats.StorageType==EStorageType::RowStorage; });

    if (optLevel == 2 && allRowStorage) {
        return node;
    }

    equiJoinCounter++;

    auto joinTuple = equiJoin.Arg(equiJoin.ArgCount() - 2).Cast<TCoEquiJoinTuple>();

    // Generate an initial tree
    auto joinTree = ConvertToJoinTree(joinTuple, rels);

    if (NYql::NLog::YqlLogger().NeedToLog(NYql::NLog::EComponent::CoreDq, NYql::NLog::ELevel::TRACE)) {
        std::stringstream str;
        str << "Converted join tree:\n";
        joinTree->Print(str);
        YQL_CLOG(TRACE, CoreDq) << str.str();
    }

    joinTree = opt.JoinSearch(joinTree, hints);

    if (NYql::NLog::YqlLogger().NeedToLog(NYql::NLog::EComponent::CoreDq, NYql::NLog::ELevel::TRACE)) {
        std::stringstream str;
        str << "Optimizied join tree:\n";
        joinTree->Print(str);
        YQL_CLOG(TRACE, CoreDq) << str.str();
    }

    // rewrite the join tree and record the output statistics
    TExprBase res = RearrangeEquiJoinTree(ctx, equiJoin, joinTree);
    typesCtx.SetStats(res.Raw(), std::make_shared<TOptimizerStatistics>(joinTree->Stats));
    return res;

}

} // namespace NYql::NDq
