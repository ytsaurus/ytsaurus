#include "implication_checker.h"

#include "misc.h"

#include <yt/yt/library/query/base/ast.h>
#include <yt/yt/library/query/base/public.h>
#include <yt/yt/library/query/base/query_common.h>

#include <library/cpp/yt/compact_containers/compact_flat_map.h>
#include <library/cpp/yt/compact_containers/compact_vector.h>

#include <util/generic/typetraits.h>

namespace NYT::NOrm::NQuery {

using namespace NQueryClient::NAst;

using NQueryClient::EBinaryOp;
using NQueryClient::TSourceLocation;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

template <class TMap>
TMap MergeMapOfVectors(TMap lhs, const TMap& rhs)
{
    for (const auto& [key, values] : rhs) {
        auto it = lhs.find(key);
        if (it == lhs.end()) {
            lhs.emplace(key, values);
        } else {
            for (const auto& value : values) {
                it->second.push_back(value);
            }
        }
    }

    return lhs;
}

////////////////////////////////////////////////////////////////////////////////

TExpressionPtr ResolveAlias(TExpressionPtr expression, const TAliasMap& aliasMap)
{
    if (auto* aliasExpression = expression->As<TAliasExpression>()) {
        return ResolveAlias(aliasExpression->Expression, aliasMap);
    }

    if (auto* referenceExpression = expression->As<TReferenceExpression>()) {
        const auto& reference = referenceExpression->Reference;
        if (!reference.TableName) {
            if (auto* aliasedExpression = MapFindPtr(aliasMap, reference.ColumnName)) {
                return ResolveAlias(*aliasedExpression, aliasMap);
            }
        }
    }

    return expression;
}

TExpressionList ResolveAlias(TExpressionList expressionList, const TAliasMap& aliasMap)
{
    for (auto& expression : expressionList) {
        expression = ResolveAlias(expression, aliasMap);
    }
    return expressionList;
}

std::string ReferenceKey(const TReference& reference)
{
    if (reference.TableName) {
        return *reference.TableName + "." + reference.ColumnName;
    }
    return reference.ColumnName;
}

TExtendedConstraints MergeConstraints(TExtendedConstraints lhs, const TExtendedConstraints& rhs)
{
    lhs.ReferenceConstraints = MergeMapOfVectors(std::move(lhs.ReferenceConstraints), rhs.ReferenceConstraints);
    for (const auto& expr : rhs.UnparsedConstraints) {
        lhs.UnparsedConstraints.push_back(expr);
    }
    return lhs;
}

////////////////////////////////////////////////////////////////////////////////

bool CheckSingleConstraintImplication(
    const TLiteralConstraint& antecedent,
    const TLiteralConstraint& consequent)
{
    static const THashMap<std::pair<EBinaryOp, EBinaryOp>, EBinaryOp> ImplicationOperatorsToProvingOperator = {
        {{EBinaryOp::Equal, EBinaryOp::Equal}, EBinaryOp::Equal},
        {{EBinaryOp::Equal, EBinaryOp::NotEqual}, EBinaryOp::NotEqual},
        {{EBinaryOp::Equal, EBinaryOp::Less}, EBinaryOp::Less},
        {{EBinaryOp::Equal, EBinaryOp::LessOrEqual}, EBinaryOp::LessOrEqual},
        {{EBinaryOp::Equal, EBinaryOp::Greater}, EBinaryOp::Greater},
        {{EBinaryOp::Equal, EBinaryOp::GreaterOrEqual}, EBinaryOp::GreaterOrEqual},

        {{EBinaryOp::NotEqual, EBinaryOp::NotEqual}, EBinaryOp::Equal},

        {{EBinaryOp::Less, EBinaryOp::NotEqual}, EBinaryOp::LessOrEqual},
        {{EBinaryOp::Less, EBinaryOp::Less}, EBinaryOp::LessOrEqual},
        {{EBinaryOp::Less, EBinaryOp::LessOrEqual}, EBinaryOp::LessOrEqual},

        {{EBinaryOp::LessOrEqual, EBinaryOp::NotEqual}, EBinaryOp::Less},
        {{EBinaryOp::LessOrEqual, EBinaryOp::Less}, EBinaryOp::Less},
        {{EBinaryOp::LessOrEqual, EBinaryOp::LessOrEqual}, EBinaryOp::LessOrEqual},

        {{EBinaryOp::Greater, EBinaryOp::NotEqual}, EBinaryOp::GreaterOrEqual},
        {{EBinaryOp::Greater, EBinaryOp::Greater}, EBinaryOp::GreaterOrEqual},
        {{EBinaryOp::Greater, EBinaryOp::GreaterOrEqual}, EBinaryOp::GreaterOrEqual},

        {{EBinaryOp::GreaterOrEqual, EBinaryOp::NotEqual}, EBinaryOp::Greater},
        {{EBinaryOp::GreaterOrEqual, EBinaryOp::Greater}, EBinaryOp::Greater},
        {{EBinaryOp::GreaterOrEqual, EBinaryOp::GreaterOrEqual}, EBinaryOp::GreaterOrEqual},
    };

    auto it = ImplicationOperatorsToProvingOperator.find(std::pair(antecedent.Operation, consequent.Operation));
    if (it == ImplicationOperatorsToProvingOperator.end()) {
        return false;
    }
    auto provingOperator = it->second;

    return Visit(antecedent.Literal, [&] (const auto& literal) {
        using T = std::decay_t<decltype(literal)>;

        if constexpr (!std::is_same_v<T, TNullLiteralValue>) {
            auto other = std::get_if<T>(&consequent.Literal);
            if (!other) {
                return false;
            }

            switch (provingOperator) {
                case EBinaryOp::Equal:
                    return literal == *other;
                case EBinaryOp::NotEqual:
                    return literal != *other;
                case EBinaryOp::Less:
                    return literal < *other;
                case EBinaryOp::LessOrEqual:
                    return literal <= *other;
                case EBinaryOp::Greater:
                    return literal > *other;
                case EBinaryOp::GreaterOrEqual:
                    return literal >= *other;
                default:
                    return false;
            }
        }
        return false;
    });
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TExtendedConstraints> CrossProduct(
    const std::vector<TExtendedConstraints>& lhs,
    const std::vector<TExtendedConstraints>& rhs)
{
    std::vector<TExtendedConstraints> result;
    result.reserve(lhs.size() * rhs.size());
    for (const auto& lhsConstraint : lhs) {
        for (const auto& rhsConstraint : rhs) {
            result.push_back(MergeConstraints(lhsConstraint, rhsConstraint));
        }
    }
    return result;
}

std::optional<std::vector<TExtendedConstraints>> ParseLexicographicComparison(
    const std::vector<TReference>& refs,
    const std::vector<TLiteralValue>& lits,
    EBinaryOp op,
    EBinaryOp crossOp)
{
    auto makeSingle = [&] (const TReference& ref, EBinaryOp constraintOp, const TLiteralValue& lit) {
        std::vector<TExtendedConstraints> result(1);
        result[0].ReferenceConstraints.emplace(
            ReferenceKey(ref), TCompactVector<TLiteralConstraint, 1>{{constraintOp, lit}});
        return result;
    };
    auto applyAnd = [&] (std::vector<TExtendedConstraints> lhs, std::vector<TExtendedConstraints> rhs) {
        if (crossOp == EBinaryOp::And) {
            return CrossProduct(lhs, rhs);
        }
        for (auto& item : rhs) {
            lhs.push_back(std::move(item));
        }
        return lhs;
    };
    auto applyOr = [&] (std::vector<TExtendedConstraints> lhs, std::vector<TExtendedConstraints> rhs) {
        if (crossOp == EBinaryOp::Or) {
            return CrossProduct(lhs, rhs);
        }
        for (auto& item : rhs) {
            lhs.push_back(std::move(item));
        }
        return lhs;
    };

    int tupleSize = std::ssize(refs);

    if (op == EBinaryOp::Equal) {
        auto result = makeSingle(refs[0], EBinaryOp::Equal, lits[0]);
        for (int tupleIndex = 1; tupleIndex < tupleSize; ++tupleIndex) {
            result = applyAnd(std::move(result), makeSingle(refs[tupleIndex], EBinaryOp::Equal, lits[tupleIndex]));
        }
        return result;
    }

    if (op == EBinaryOp::Greater || op == EBinaryOp::GreaterOrEqual ||
        op == EBinaryOp::Less || op == EBinaryOp::LessOrEqual)
    {
        bool isGreater = (op == EBinaryOp::Greater || op == EBinaryOp::GreaterOrEqual);
        EBinaryOp strictOp = isGreater ? EBinaryOp::Greater : EBinaryOp::Less;
        EBinaryOp lastOp = op;

        auto result = makeSingle(refs[0], tupleSize == 1 ? lastOp : strictOp, lits[0]);
        for (int tupleIndex = 1; tupleIndex < tupleSize; ++tupleIndex) {
            auto branch = makeSingle(refs[0], EBinaryOp::Equal, lits[0]);
            for (int prefixIndex = 1; prefixIndex < tupleIndex; ++prefixIndex) {
                branch = applyAnd(std::move(branch), makeSingle(refs[prefixIndex], EBinaryOp::Equal, lits[prefixIndex]));
            }
            branch = applyAnd(
                std::move(branch),
                makeSingle(refs[tupleIndex], tupleIndex == tupleSize - 1 ? lastOp : strictOp, lits[tupleIndex]));
            result = applyOr(std::move(result), std::move(branch));
        }
        return result;
    }

    return std::nullopt;
}

std::vector<TExtendedConstraints> ParseToNormalForm(
    TExpressionPtr expression,
    const TAliasMap& aliasMap,
    EBinaryOp crossOp,
    EBinaryOp unionOp)
{
    if (auto* aliasExpression = expression->As<TAliasExpression>()) {
        return ParseToNormalForm(aliasExpression->Expression, aliasMap, crossOp, unionOp);
    }

    if (auto* referenceExpression = expression->As<TReferenceExpression>()) {
        if (!referenceExpression->Reference.TableName) {
            if (auto* resolved = MapFindPtr(aliasMap, referenceExpression->Reference.ColumnName)) {
                return ParseToNormalForm(*resolved, aliasMap, crossOp, unionOp);
            }
        }
    }

    auto recurse = [&] (TExpressionPtr expr) {
        return ParseToNormalForm(expr, aliasMap, crossOp, unionOp);
    };

    if (const auto* binary = expression->As<TBinaryOpExpression>()) {
        if (binary->Opcode == crossOp) {
            return CrossProduct(recurse(binary->Lhs[0]), recurse(binary->Rhs[0]));
        }

        if (binary->Opcode == unionOp) {
            auto result = recurse(binary->Lhs[0]);
            for (auto& constraint : recurse(binary->Rhs[0])) {
                result.push_back(std::move(constraint));
            }
            return result;
        }

        auto opcode = binary->Opcode;
        auto resolvedLhs = ResolveAlias(binary->Lhs, aliasMap);
        auto resolvedRhs = ResolveAlias(binary->Rhs, aliasMap);

        auto ref = TryExtractReference(resolvedLhs);
        std::optional<TLiteralValue> lit;

        if (ref) {
            lit = TryExtractSingleLiteralValue(resolvedRhs);
        } else {
            ref = TryExtractReference(resolvedRhs);
            if (ref) {
                lit = TryExtractSingleLiteralValue(resolvedLhs);
                opcode = NQueryClient::GetReversedBinaryOpcode(opcode);
            }
        }

        if (ref && lit) {
            TExtendedConstraints constraints;
            constraints.ReferenceConstraints.emplace(ReferenceKey(*ref), TCompactVector<TLiteralConstraint, 1>{{opcode, *lit}});
            return {std::move(constraints)};
        }

        // Multi-column tuple comparison: (refs...) OP (lits...) or reversed.
        if (binary->Lhs.size() > 1 && binary->Lhs.size() == binary->Rhs.size()) {
            auto tupleOp = binary->Opcode;
            std::vector<TReference> tupleRefs;
            std::vector<TLiteralValue> tupleLits;

            auto tryExtractTuple = [&] (const TExpressionList& refSide, const TExpressionList& litSide) {
                tupleRefs.clear();
                tupleLits.clear();
                for (int elementIndex = 0; elementIndex < std::ssize(refSide); ++elementIndex) {
                    auto elementRef = TryExtractReference({refSide[elementIndex]});
                    auto elementLit = TryExtractSingleLiteralValue({litSide[elementIndex]});
                    if (!elementRef || !elementLit) {
                        return false;
                    }
                    tupleRefs.push_back(*elementRef);
                    tupleLits.push_back(*elementLit);
                }
                return true;
            };

            bool tupleValid = tryExtractTuple(resolvedLhs, resolvedRhs);
            if (!tupleValid) {
                tupleOp = NQueryClient::GetReversedBinaryOpcode(tupleOp);
                tupleValid = tryExtractTuple(resolvedRhs, resolvedLhs);
            }

            if (tupleValid) {
                if (auto parsed = ParseLexicographicComparison(tupleRefs, tupleLits, tupleOp, crossOp)) {
                    return std::move(*parsed);
                }
            }
        }
    }

    if (const auto* inExpr = expression->As<TInExpression>()) {
        auto resolvedExpr = ResolveAlias(inExpr->Expr, aliasMap);
        auto ref = TryExtractReference(resolvedExpr);
        if (ref) {
            // IN is OR of equalities; apply the OR operation (union or cross-product).
            std::vector<TExtendedConstraints> result;
            bool fallback = false;
            for (const auto& value : inExpr->Values) {
                if (value.size() != 1) {
                    fallback = true;
                    break;
                }
                TExtendedConstraints constraints;
                constraints.ReferenceConstraints.emplace(
                    ReferenceKey(*ref),
                    TCompactVector<TLiteralConstraint, 1>{{EBinaryOp::Equal, value[0]}});
                if (result.empty()) {
                    result.push_back(std::move(constraints));
                } else if (crossOp == EBinaryOp::Or) {
                    result = CrossProduct(result, {{std::move(constraints)}});
                } else {
                    result.push_back(std::move(constraints));
                }
            }
            if (!fallback) {
                return result;
            }
        }
    }

    TExtendedConstraints constraints;
    constraints.UnparsedConstraints.push_back(expression);
    return {std::move(constraints)};
}

////////////////////////////////////////////////////////////////////////////////

bool CheckConjunctionImpliesDisjunction(
    const TExtendedConstraints& conjunction,
    const TExtendedConstraints& disjunction)
{
    for (const auto& [ref, disjunctConstraints] : disjunction.ReferenceConstraints) {
        for (const auto& disjunct : disjunctConstraints) {
            auto it = conjunction.ReferenceConstraints.find(ref);
            if (it != conjunction.ReferenceConstraints.end()) {
                for (const auto& conjunctionConstraints : it->second) {
                    if (CheckSingleConstraintImplication(conjunctionConstraints, disjunct)) {
                        return true;
                    }
                }
            }
        }
    }

    for (const auto& unparsedDisjunct : disjunction.UnparsedConstraints) {
        for (const auto& unparsedConjunct : conjunction.UnparsedConstraints) {
            if (*unparsedConjunct == *unparsedDisjunct) {
                return true;
            }
        }
    }

    return false;
}

bool CheckImplicationNormalForm(
    TDisjunctiveNormalForm dnfAntecedent,
    TConjunctiveNormalForm cnfConsequent)
{
    for (const auto& disjunction : cnfConsequent.Disjunctions) {
        for (const auto& conjunction : dnfAntecedent.Conjunctions) {
            if (!CheckConjunctionImpliesDisjunction(conjunction, disjunction)) {
                return false;
            }
        }
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TDisjunctiveNormalForm ParseToDNF(TExpressionPtr expression, const TAliasMap& aliasMap)
{
    return {ParseToNormalForm(expression, aliasMap, EBinaryOp::And, EBinaryOp::Or)};
}

TConjunctiveNormalForm ParseToCNF(TExpressionPtr expression, const TAliasMap& aliasMap)
{
    return {ParseToNormalForm(expression, aliasMap, EBinaryOp::Or, EBinaryOp::And)};
}

bool CheckImplication(
    TExpressionPtr antecedent,
    TExpressionPtr consequent,
    const TAliasMap& antecedentAliases,
    const TAliasMap& consequentAliases)
{
    return CheckImplicationNormalForm(
        ParseToDNF(antecedent, antecedentAliases),
        ParseToCNF(consequent, consequentAliases));
}

bool CheckImplication(
    TDisjunctiveNormalForm dnfAntecedent,
    TConjunctiveNormalForm cnfConsequent)
{
    return CheckImplicationNormalForm(dnfAntecedent, cnfConsequent);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
