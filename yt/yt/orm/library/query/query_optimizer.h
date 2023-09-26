#pragma once

#include "public.h"

#include <yt/yt/library/query/base/ast.h>

namespace NYT::NOrm::NQuery {

////////////////////////////////////////////////////////////////////////////////

//! Executes optimizers for queries with join statement.
//! Returns true, if query was modified, otherwise false.
bool TryOptimizeJoin(NQueryClient::NAst::TQuery* query);

//! Executes optimizers for queries, which have filter by prefix and group-by by suffix_key.
/*! E.g. Table has the following format: [prefix_key, suffix_key, etc...]
 *  and query is following:
 *      SELECT FIRST(suffix_key) WHERE prefix_key IN (...) GROUP BY suffix_key.
 *  This GROUP BY expression is done to eliminate possible duplicates
 *  when prefix_key is constrained by a range of values.
 *  In case when prefix is unique, group-by clause is not needed.
 */
bool TryOptimizeGroupByWithUniquePrefix(
    NQueryClient::NAst::TExpressionPtr filterExpression,
    const std::vector<TString>& prefixReferences,
    const TString& tableName);

//! Executes optimizer for queries, transforming string(try_get_string(...))
//! into try_get_string(...).
bool TryOptimizeTryGetString(NQueryClient::NAst::TQuery* query);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
