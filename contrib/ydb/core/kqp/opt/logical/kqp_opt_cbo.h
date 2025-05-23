#pragma once

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/cbo/cbo_optimizer_new.h>

#include <contrib/ydb/core/kqp/opt/kqp_opt.h>

namespace NKikimr::NKqp::NOpt {

/**
 * KQP specific Rel node, includes a pointer to ExprNode
*/
struct TKqpRelOptimizerNode : public NYql::TRelOptimizerNode {
    const NYql::TExprNode::TPtr Node;

    TKqpRelOptimizerNode(TString label, NYql::TOptimizerStatistics stats, const NYql::TExprNode::TPtr node) : 
        TRelOptimizerNode(label, std::move(stats)), Node(node) { }
};

/**
 * KQP Specific cost function and join applicability cost function
*/
struct TKqpProviderContext : public NYql::TBaseProviderContext {
    TKqpProviderContext(const TKqpOptimizeContext& kqpCtx, const int optLevel) : KqpCtx(kqpCtx), OptLevel(optLevel) {}

    virtual bool IsJoinApplicable(
        const std::shared_ptr<NYql::IBaseOptimizerNode>& left, 
        const std::shared_ptr<NYql::IBaseOptimizerNode>& right, 
        const TVector<NYql::NDq::TJoinColumn>& leftJoinKeys, 
        const TVector<NYql::NDq::TJoinColumn>& rightJoinKeys,
        NYql::EJoinAlgoType joinAlgo,  
        NYql::EJoinKind joinKind
    ) override;

    virtual double ComputeJoinCost(
        const NYql::TOptimizerStatistics& leftStats, 
        const NYql::TOptimizerStatistics& rightStats, 
        const double outputRows, 
        const double outputByteSize, 
        NYql::EJoinAlgoType joinAlgo
    ) const override;

    const TKqpOptimizeContext& KqpCtx;
    int OptLevel;
};

}
