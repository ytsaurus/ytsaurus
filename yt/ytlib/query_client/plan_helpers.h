#pragma once

#include "public.h"
#include "key_trie.h"

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/row_buffer.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TKeyTrieNode ExtractMultipleConstraints(
    const TExpression* expr,
    const TKeyColumns& keyColumns,
    TRowBuffer* rowBuffer);

////////////////////////////////////////////////////////////////////////////////

//! Infers key columns of the query result.
//! XXX(sandello): Right now it just computes key columns for the data source.
TKeyColumns InferKeyColumns(const TOperator* op);

//! Refines |keyRange| for key composed of |keyColumns| given
//! the filtering predicate.
TKeyRange RefineKeyRange(
    const TKeyColumns& keyColumns,
    const TKeyRange& keyRange,
    const TExpression* predicate);

//! Returns a minimal key range that cover both inputs.
TKeyRange Unite(const TKeyRange& first, const TKeyRange& second);

//! Returns a maximal key range covered by both inputs.
TKeyRange Intersect(const TKeyRange& first, const TKeyRange& second);

//! Checks whether key range is empty.
bool IsEmpty(const TKeyRange& keyRange);

//! Infers the resulting type of the expression.
EValueType InferType(const TExpression* expr, const TTableSchema& sourceSchema);

//! Infers the column name for the expression.
Stroka InferName(const TExpression* expr);

//! Checks whether a given expression yields a constant value.
bool IsConstant(const TExpression* expr);

//! Computes an expression value given that it is constant.
TValue GetConstantValue(const TExpression* expr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

