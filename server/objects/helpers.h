#pragma once

#include "public.h"

#include <yt/ytlib/query_client/ast.h>

#include <yt/core/ypath/public.h>

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

struct TResolveResult
{
    TAttributeSchema* Attribute;
    NYT::NYPath::TYPath SuffixPath;
};

TResolveResult ResolveAttribute(
    IObjectTypeHandler* typeHandler,
    const NYT::NYPath::TYPath& path);

////////////////////////////////////////////////////////////////////////////////

struct TAttributeFetcherContext
{
    NYT::NQueryClient::NAst::TExpressionList SelectExprs;
    int ObjectIdIndex = -1;
    int ParentIdIndex = -1;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EAttributeFetchMethod,
    (Composite)
    (ExpressionBuilder)
    (Evaluator)
);

class TAttributeFetcher
{
public:
    TAttributeFetcher(
        IObjectTypeHandler* typeHandler,
        const TResolveResult& resolveResult,
        TTransactionPtr transaction,
        TAttributeFetcherContext* fetcherContext,
        IQueryContext* queryContext);

    void Prefetch(NYT::NTableClient::TUnversionedRow row);
    NYT::NYson::TYsonString Fetch(NYT::NTableClient::TUnversionedRow row);

private:
    IObjectTypeHandler* const TypeHandler_;
    const TResolveResult RootResolveResult_;
    const TTransactionPtr Transaction_;
    TAttributeFetcherContext* const FetcherContext_;
    const int StartIndex_;
    
    int CurrentIndex_;

    static EAttributeFetchMethod GetFetchMethod(const TResolveResult& resolveResult);

    void DoPrepare(
        const TResolveResult& resolveResult,
        IQueryContext* queryContext);
    void DoPrefetch(
        NYT::NTableClient::TUnversionedRow row,
        const TResolveResult& resolveResult);
    void DoFetch(
        NYT::NTableClient::TUnversionedRow row,
        const TResolveResult& resolveResult,
        NYson::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

NYT::NQueryClient::NAst::TExpressionPtr BuildFilterExpression(
    IObjectTypeHandler* typeHandler,
    IQueryContext* context,
    const TObjectFilter& filter);

NYT::NQueryClient::NAst::TExpressionPtr BuildAndExpression(
    NYT::NQueryClient::NAst::TExpressionPtr lhs,
    NYT::NQueryClient::NAst::TExpressionPtr rhs);

////////////////////////////////////////////////////////////////////////////////

TStringBuf GetCapitalizedHumanReadableTypeName(EObjectType type);
TStringBuf GetLowercaseHumanReadableTypeName(EObjectType type);

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP
