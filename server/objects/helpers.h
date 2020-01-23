#pragma once

#include "public.h"

#include <yp/server/access_control/public.h>

#include <yt/client/api/client.h>

#include <yt/ytlib/query_client/ast.h>

#include <yt/core/ypath/public.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct TResolveResult
{
    TAttributeSchema* Attribute;
    NYT::NYPath::TYPath SuffixPath;
};

struct TResolvePermissions
{
    std::vector<NAccessControl::EAccessControlPermission> ReadPermissions;
};

TResolveResult ResolveAttribute(
    IObjectTypeHandler* typeHandler,
    const NYT::NYPath::TYPath& path,
    TResolvePermissions* permissions = nullptr);

////////////////////////////////////////////////////////////////////////////////

class TAttributeFetcherContext
{
public:
    explicit TAttributeFetcherContext(IQueryContext* queryContext);

    void AddSelectExpression(NYT::NQueryClient::NAst::TExpressionPtr expr);
    const NYT::NQueryClient::NAst::TExpressionList& GetSelectExpressions() const;

    TObjectId GetObjectId(NYT::NTableClient::TUnversionedRow row) const;
    TObjectId GetParentId(NYT::NTableClient::TUnversionedRow row) const;

    TObject* GetObject(
        TTransaction* transaction,
        NYT::NTableClient::TUnversionedRow row) const;
    std::vector<TObject*> GetObjects(
        TTransaction* transaction,
        TRange<NYT::NTableClient::TUnversionedRow> rows) const;

    NYT::NTableClient::TUnversionedValue RetrieveNextValue(
        NYT::NTableClient::TUnversionedRow row,
        int* currentIndex) const;

private:
    IQueryContext* const QueryContext_;

    NYT::NQueryClient::NAst::TExpressionList SelectExprs_;
    int ObjectIdIndex_ = -1;
    int ParentIdIndex_ = -1;

    int RegisterField(const TDBField* field);
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
        const TResolveResult& resolveResult,
        TTransaction* transaction,
        TAttributeFetcherContext* fetcherContext,
        IQueryContext* queryContext);

    void Prefetch(NYT::NTableClient::TUnversionedRow row);
    NYT::NYson::TYsonString Fetch(NYT::NTableClient::TUnversionedRow row);

private:
    const TResolveResult RootResolveResult_;
    TTransaction* const Transaction_;
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
    IQueryContext* context,
    const TObjectFilter& filter);

NYT::NQueryClient::NAst::TExpressionPtr BuildAndExpression(
    NYT::NQueryClient::NAst::TExpressionPtr lhs,
    NYT::NQueryClient::NAst::TExpressionPtr rhs);

NYT::NQueryClient::NAst::TExpressionList RewriteExpressions(
    IQueryContext* context,
    const std::vector<TString>& expressions);

////////////////////////////////////////////////////////////////////////////////

TString GetObjectDisplayName(const TObject* object);

TObjectId GenerateId(const TObjectId& id);

////////////////////////////////////////////////////////////////////////////////

void ValidateSubjectExists(TTransaction* transaction, const TObjectId& subjectId);

////////////////////////////////////////////////////////////////////////////////

TTimestamp GetBarrierTimestamp(const std::vector<NYT::NApi::TTabletInfo>& tabletInfos);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
