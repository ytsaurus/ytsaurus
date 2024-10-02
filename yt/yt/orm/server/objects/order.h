#pragma once

#include "public.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct TObjectOrderByExpression
{
    // Examples: `[/meta/id]`, `[/meta/creation_time] + 100`.
    TString Expression;
    bool Descending = false;

    bool operator==(const TObjectOrderByExpression& other) const = default;
};

void FormatValue(TStringBuilderBase* builder, const TObjectOrderByExpression& expression, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

struct TObjectOrderBy
{
    std::vector<TObjectOrderByExpression> Expressions;
};

void FormatValue(TStringBuilderBase* builder, const TObjectOrderBy& orderBy, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
