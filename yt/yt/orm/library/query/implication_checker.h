#pragma once

#include "public.h"

#include <library/cpp/yt/compact_containers/compact_vector.h>
#include <yt/yt/library/query/base/ast.h>

namespace NYT::NOrm::NQuery {

////////////////////////////////////////////////////////////////////////////////

struct TLiteralConstraint
{
    NQueryClient::EBinaryOp Operation;
    NQueryClient::NAst::TLiteralValue Literal;
};

struct TConstraints
{
    TCompactFlatMap<std::string, TCompactVector<TLiteralConstraint, 1>, 2> ReferenceConstraints;
};

struct TExtendedConstraints
    : public TConstraints
{
    std::vector<NQueryClient::NAst::TExpressionPtr> UnparsedConstraints;
};

struct TConjunctiveNormalForm
{
    std::vector<TExtendedConstraints> Disjunctions;
};

struct TDisjunctiveNormalForm
{
    std::vector<TExtendedConstraints> Conjunctions;
};

////////////////////////////////////////////////////////////////////////////////

TDisjunctiveNormalForm ParseToDNF(
    NQueryClient::NAst::TExpressionPtr expression,
    const NQueryClient::NAst::TAliasMap& aliasMap = {});
TConjunctiveNormalForm ParseToCNF(
    NQueryClient::NAst::TExpressionPtr expression,
    const NQueryClient::NAst::TAliasMap& aliasMap = {});

////////////////////////////////////////////////////////////////////////////////

bool CheckImplication(
    NQueryClient::NAst::TExpressionPtr antecedent,
    NQueryClient::NAst::TExpressionPtr consequent,
    const NQueryClient::NAst::TAliasMap& antecedentAliases = {},
    const NQueryClient::NAst::TAliasMap& consequentAliases = {});

bool CheckImplication(
    TDisjunctiveNormalForm dnfAntecedent,
    TConjunctiveNormalForm cnfConsequent);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
