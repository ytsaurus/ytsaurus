#include "yql_ytflow_swift_map.h"

#include "yql_ytflow_constants.h"

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_opt_utils.h>

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>


namespace NYql::NYtflow::NPrivate {

using namespace NNodes;

namespace {

bool HasSwiftIncompatibleExpressions(const TExprNode::TPtr& lambda)
{
    return static_cast<bool>(FindNode(lambda, [](const TExprNode::TPtr& node) {
        return TCoNonDeterministicBase::Match(node.Get())
            || TCoDependsOnBase::Match(node.Get());
    }));
}

bool IsSwiftMapEligible(const TYtflowMapBase& map, bool hasNonDeterministicFunctions)
{
    // TYtflowExtend has non-empty sources and sinks after type annotation. Renaming
    // it to a map implementation preserves these properties, while its pipeline-spec
    // lowering cannot add timers, key visitors, a watermark generator or watermark
    // alignment. The remaining Swift-map requirements are checked explicitly below.
    const auto lambda = map.Lambda().Ptr();
    if (hasNonDeterministicFunctions || lambda->HasSideEffects() || HasSwiftIncompatibleExpressions(lambda)) {
        return false;
    }

    for (auto source : map.Sources()) {
        if (!source.Maybe<TYtflowOutput>()) {
            return false;
        }
    }

    for (auto sink : map.Sinks()) {
        if (!sink.Maybe<TYtflowIntermediateSink>()) {
            return false;
        }
    }

    return true;
}

} // anonymous namespace

TExprNode::TPtr SelectExtendImplementation(
    const TExprNode::TPtr& operation,
    bool hasNonDeterministicFunctions,
    TExprContext& ctx)
{
    const bool isExtend = TYtflowExtend::Match(operation.Get());
    const bool isTransformMap = TYtflowTransformMap::Match(operation.Get());
    const bool isSwiftMap = TYtflowSwiftMap::Match(operation.Get());
    const bool hasExtendSetting = (isTransformMap || isSwiftMap) && HasSetting(
        *operation->Child(TYtflowMapBase::idx_Settings),
        EXTEND_SETTING);
    if (!isExtend && !hasExtendSetting) {
        return operation;
    }

    const bool isEligible = IsSwiftMapEligible(
        TYtflowMapBase(operation),
        hasNonDeterministicFunctions);
    if ((isEligible && isSwiftMap) || (!isEligible && isTransformMap)) {
        return operation;
    }

    auto selectedOperation = operation;
    if (!hasExtendSetting) {
        auto settings = AddSetting(
            *operation->Child(TYtflowMapBase::idx_Settings),
            operation->Pos(),
            TString(EXTEND_SETTING),
            nullptr,
            ctx);
        selectedOperation = ctx.ChangeChild(
            *operation,
            TYtflowMapBase::idx_Settings,
            std::move(settings));
    }

    return ctx.RenameNode(
        *selectedOperation,
        isEligible ? TYtflowSwiftMap::CallableName() : TYtflowTransformMap::CallableName());
}

} // namespace NYql::NYtflow::NPrivate
