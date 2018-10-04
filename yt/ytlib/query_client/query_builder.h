#pragma once

#include "public.h"

#include <yt/core/misc/nullable.h>

#include <util/generic/string.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EOrderByDirection,
    (Ascending)
    (Descending)
);

////////////////////////////////////////////////////////////////////////////////

class TQueryBuilder
{
public:
    void SetSource(TString source);

    int AddSelectExpression(TString expression, TNullable<TString> alias = {});
    // NB(levysotsky): This overload is necessary to allow calling AddSelectExpression(TString, const char*).
    int AddSelectExpression(TString expression, TString alias);

    void AddWhereConjunct(TString expression);

    void AddGroupByExpression(TString expression, TNullable<TString> alias = {});
    // NB(levysotsky): This overload is necessary to allow calling AddGroupByExpression(TString, const char*).
    void AddGroupByExpression(TString expression, TString alias);

    void AddOrderByExpression(TString expression, TNullable<EOrderByDirection> direction = {});
    void AddOrderByAscendingExpression(TString expression);
    void AddOrderByDescendingExpression(TString expression);

    void SetLimit(i64 limit);

    TString Build();

private:
    struct TEntryWithAlias
    {
        TString Expression;
        TNullable<TString> Alias;
    };

    struct TOrderByEntry
    {
        TString Expression;
        TNullable<EOrderByDirection> Direction;
    };

private:
    TNullable<TString> Source_;
    std::vector<TEntryWithAlias> SelectEntries_;
    std::vector<TString> WhereConjuncts_;
    std::vector<TOrderByEntry> OrderByEntries_;
    std::vector<TEntryWithAlias> GroupByEntries_;
    TNullable<i64> Limit_;

private:
    // We overload this functions to allow the corresponding JoinSeq().
    friend void AppendToString(TString& dst, const TEntryWithAlias& entry);
    friend void AppendToString(TString& dst, const TOrderByEntry& entry);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
