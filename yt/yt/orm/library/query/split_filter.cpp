#include "split_filter.h"

#include <yt/yt/library/query/base/ast_visitors.h>
#include <yt/yt/library/query/base/ast.h>
#include <yt/yt/library/query/misc/objects_holder.h>

#include <yt/yt/orm/client/misc/error.h>

#include <optional>

using namespace NYT::NQueryClient::NAst;

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

namespace {

DEFINE_ENUM(EFilterPlace,
    // Subexpression place cannot be deduced without additional context.
    (Unknown)
    // Subexpression should be put in where clause of select query.
    (Where)
    // Subexpression should be put in having clause of select query.
    (Having)
    // Subexpression is a join predicate.
    (JoinPredicate)
    // Subexpression contains expression belonging to different parts of select query.
    (Heterogenous)
);

struct TFilterType
{
    EFilterPlace Place = EFilterPlace::Unknown;
    TString JoinName;

    bool operator==(const TFilterType& other) const = default;
};

class TFilterSplitter
    : public TBaseAstVisitor<TFilterType, TFilterSplitter>
{
public:
    using TBase = TBaseAstVisitor<TFilterType, TFilterSplitter>;

    TFilterSplitter(
        TObjectsHolder* objectsHolder,
        const TFilterHints& filterHints)
        : ObjectsHolder_(objectsHolder)
        , FilterHints_(filterHints)
    { }

    TFilterType OnLiteral(const TLiteralExpressionPtr /*literalExpr*/)
    {
        return {};
    }

    TFilterType OnReference(const TReferenceExpressionPtr /*referenceExpr*/)
    {
        return {.Place = EFilterPlace::Where};
    }

    TFilterType OnAlias(const TAliasExpressionPtr aliasExpr)
    {
        auto type = Visit(aliasExpr->Expression);
        if (type.Place == EFilterPlace::Heterogenous) {
            ThrowSplitError(TError("Cannot alias heterogenous expression in filter")
                << TErrorAttribute("expression", FormatExpression(*aliasExpr->Expression)));
        }
        return type;
    }

    TFilterType OnUnary(const TUnaryOpExpressionPtr unaryExpr)
    {
        auto type = Visit(unaryExpr->Operand);
        if (type.Place == EFilterPlace::Heterogenous) {
            ThrowSplitError(TError("Cannot apply unary %lv on heterogenous expression in filter", unaryExpr->Opcode)
                << TErrorAttribute("expression", FormatExpression(unaryExpr->Operand)));
        }
        return type;
    }

    TFilterType OnBinary(const TBinaryOpExpressionPtr binaryExpr)
    {
        const auto& lhs = binaryExpr->Lhs;
        auto lhsType = Visit(lhs);
        const auto& rhs = binaryExpr->Rhs;
        auto rhsType = Visit(rhs);

        auto commonType = GetCommonType(lhsType, rhsType);

        if (commonType.Place != EFilterPlace::Heterogenous) {
            return commonType;
        }

        if (binaryExpr->Opcode == NQueryClient::EBinaryOp::And) {
            AddExpression(lhs[0], lhsType);
            AddExpression(rhs[0], rhsType);
        } else {
            ThrowSplitError(TError("Cannot apply binary %lv to heterogenous operands", binaryExpr->Opcode)
                << TErrorAttribute("expression", FormatExpression(*binaryExpr)));
        }

        return commonType;
    }

    TFilterType OnFunction(const TFunctionExpressionPtr functionExpr)
    {
        auto type = Visit(functionExpr->Arguments);
        if (type.Place == EFilterPlace::Heterogenous) {
            ThrowSplitError(TError("Cannot call function %v with heterogenous arguments", functionExpr->FunctionName)
                << TErrorAttribute("expression", FormatExpression(*functionExpr)));
        }
        return type;
    }

    TFilterType OnIn(const TInExpressionPtr inExpr)
    {
        auto type = Visit(inExpr->Expr);
        if (type.Place == EFilterPlace::Heterogenous) {
            ThrowSplitError(TError("Cannot use in with heterogenous expression")
                << TErrorAttribute("expression", FormatExpression(*inExpr)));
        }
        return type;
    }

    TFilterType OnBetween(const TBetweenExpressionPtr betweenExpr)
    {
        auto type = Visit(betweenExpr->Expr);
        if (type.Place == EFilterPlace::Heterogenous) {
            ThrowSplitError(TError("Cannot use between with heterogenous expression")
                << TErrorAttribute("expression", FormatExpression(*betweenExpr)));
        }
        return type;
    }

    TFilterType OnTransform(const TTransformExpressionPtr transformExpr)
    {
        const auto& expr = transformExpr->Expr;
        auto exprType = Visit(expr);
        const auto& defaultExpr = transformExpr->DefaultExpr;
        auto defaultExprType = Visit(defaultExpr);
        auto type = GetCommonType(exprType, defaultExprType);

        if (type.Place == EFilterPlace::Heterogenous) {
            ThrowSplitError(TError("Cannot use transform with heterogenous arguments")
                << TErrorAttribute("expression", FormatExpression(*transformExpr)));
        }

        return type;
    }

    TFilterType OnCase(const TCaseExpressionPtr caseExpr)
    {
        auto optionalType = Visit(caseExpr->OptionalOperand);
        auto defaultType = Visit(caseExpr->DefaultExpression);
        std::vector<TFilterType> types;
        types.push_back(optionalType);
        types.push_back(defaultType);
        for (const auto& whenThenExpr : caseExpr->WhenThenExpressions) {
            for (const auto& expr : whenThenExpr.Condition) {
                types.push_back(Visit(expr));
            }
            for (const auto& expr : whenThenExpr.Result) {
                types.push_back(Visit(expr));
            }
        }
        auto commonType = GetCommonType(types);
        if (commonType.Place == EFilterPlace::Heterogenous) {
            ThrowSplitError(TError("Cannot use case with heterogenous arguments")
                << TErrorAttribute("expression", FormatExpression(*caseExpr)));
        }
        return commonType;
    }

    TFilterType OnLike(const TLikeExpressionPtr likeExpr)
    {
        auto textType = Visit(likeExpr->Text);
        auto patternType = Visit(likeExpr->Pattern);
        auto escapeType = Visit(likeExpr->EscapeCharacter);

        auto commonType = GetCommonType(std::array{textType, patternType, escapeType});

        if (commonType.Place == EFilterPlace::Heterogenous) {
            ThrowSplitError(TError("Cannot use like with heterogenous arguments")
                << TErrorAttribute("expression", FormatExpression(*likeExpr)));
        }

        return commonType;
    }

    TFilterType Visit(TExpressionPtr expression)
    {
        auto type = GetExpressionFilterTypeHint(expression);
        if (type.Place != EFilterPlace::Unknown) {
            return type;
        }
        type = TBase::Visit(expression);
        return type;
    }

    TFilterType Visit(const std::vector<TExpressionPtr>& tuple)
    {
        return GetCommonType(tuple | std::views::transform([this] (TExpressionPtr expr) {
            return Visit(expr);
        }));
    }

    TFilterType Visit(const std::optional<std::vector<TExpressionPtr>>& nullableTuple)
    {
        if (nullableTuple) {
            return Visit(*nullableTuple);
        }
        return {};
    }

    TFilterSplit Split(TExpressionPtr expression)
    {
        FilterExpression_ = expression;
        auto type = Visit(expression);
        switch (type.Place) {
            case EFilterPlace::Unknown:
            case EFilterPlace::Where:
                Where_ = expression;
                break;
            case EFilterPlace::Having:
                Having_ = expression;
                break;
            case EFilterPlace::JoinPredicate:
                JoinPredicates_[type.JoinName] = expression;
                break;
            case EFilterPlace::Heterogenous:
                break;
        }
        TFilterSplit split;
        if (Where_ != nullptr) {
            split.Where = TExpressionList{Where_};
        }
        if (Having_ != nullptr) {
            split.Having = TExpressionList{Having_};
        }
        for (auto& [name, expr] : JoinPredicates_) {
            split.JoinPredicates[name] = TExpressionList{expr};
        }
        return split;
    }

private:
    TObjectsHolder* const ObjectsHolder_;
    const TFilterHints& FilterHints_;

    TExpressionPtr Where_ = nullptr;
    TExpressionPtr Having_ = nullptr;
    THashMap<TString, TExpressionPtr> JoinPredicates_;

    TExpressionPtr FilterExpression_;

    TFilterType GetExpressionFilterTypeHint(
        TExpressionPtr expression,
        bool isReference = false)
    {
        auto joinIt = FilterHints_.JoinPredicates.find(expression);
        if (joinIt != FilterHints_.JoinPredicates.end()) {
            return {
                .Place = EFilterPlace::JoinPredicate,
                .JoinName = joinIt->second,
            };
        }

        if (FilterHints_.Having.contains(expression)) {
            return {.Place=EFilterPlace::Having};
        }

        return isReference
            ? TFilterType{.Place = EFilterPlace::Where}
            : TFilterType{.Place = EFilterPlace::Unknown};
    }

    TFilterType GetCommonType(
        const TFilterType& lhs,
        const TFilterType& rhs)
    {
        if (lhs.Place == EFilterPlace::Unknown) {
            return rhs;
        }

        if (rhs.Place == EFilterPlace::Unknown) {
            return lhs;
        }

        if (lhs == rhs) {
            return lhs;
        }

        return {.Place=EFilterPlace::Heterogenous};
    }

    template<std::ranges::range TFilterTypes>
    TFilterType GetCommonType(TFilterTypes types)
    {
        TFilterType commonType;

        for (const auto& type : types) {
            commonType = GetCommonType(commonType, type);
            if (commonType.Place == EFilterPlace::Heterogenous) {
                return commonType;
            }
        }

        return commonType;
    }

    void AddExpression(TExpressionPtr expression, const TFilterType& type)
    {
        switch (type.Place) {
            case EFilterPlace::Having:
                Having_ = BuildAndExpression(ObjectsHolder_, Having_, expression);
                return;
            case EFilterPlace::Where:
                Where_ = BuildAndExpression(ObjectsHolder_, Where_, expression);
                return;
            case EFilterPlace::JoinPredicate: {
                YT_VERIFY(!type.JoinName.empty());
                JoinPredicates_[type.JoinName] = BuildAndExpression(
                    ObjectsHolder_,
                    JoinPredicates_[type.JoinName],
                    expression);
                return;
            }
            case EFilterPlace::Heterogenous:
                return;
            case EFilterPlace::Unknown:
                ThrowSplitError(TError("Failed to determine filter place")
                    << TErrorAttribute("expression", FormatExpression(*expression)));
        }
    }

    void ThrowSplitError(TError error)
    {
        THROW_ERROR_EXCEPTION(
            NClient::EErrorCode::InvalidRequestArguments,
            "Cannot split query filter")
                << std::move(error)
                << TErrorAttribute("filter", FormatExpression(*FilterExpression_));
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

TFilterSplit SplitFilter(
    TExpressionPtr filterExpression,
    const TFilterHints& filterHints,
    TObjectsHolder* objectsHolder)
{
    return TFilterSplitter(objectsHolder, filterHints).Split(filterExpression);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
