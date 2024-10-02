#pragma once

#include "public.h"

#include <yt/yt/orm/client/objects/type.h>

#include <yt/yt/library/query/base/ast.h>

#include <yt/yt/client/api/client.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TFetcherContext
{
public:
    TFetcherContext(IQueryContext* queryContext, TTransaction* transaction, bool forceReadUncommittedChanges = false);

    void RegisterKeyAndParentKeyFields(IObjectTypeHandler* typeHandler);

    void AddSelectExpression(NQueryClient::NAst::TExpressionPtr expr);
    const NQueryClient::NAst::TExpressionList& GetSelectExpressions() const;

    std::optional<std::vector<const TDBField*>> ExtractLookupFields(const TDBTable* primaryTable) const;

    TObjectKey GetObjectKey(NTableClient::TUnversionedRow row) const;
    TObjectKey GetParentKey(NTableClient::TUnversionedRow row) const;

    TObject* GetObject(
        NTableClient::TUnversionedRow row) const;
    std::vector<TObject*> GetObjects(
        TRange<NTableClient::TUnversionedRow> rows) const;

    DEFINE_BYVAL_RO_PROPERTY_NO_INIT(IQueryContext*, QueryContext);
    DEFINE_BYVAL_RO_PROPERTY_NO_INIT(TTransaction*, Transaction);
    DEFINE_BYREF_RO_PROPERTY_NO_INIT(bool, ForceReadUncommittedChanges);

    DEFINE_BYREF_RW_PROPERTY(NTableClient::EVersionedIOMode, VersionedIOMode);

private:
    NQueryClient::NAst::TExpressionList SelectExprs_;
    std::vector<int> KeyFieldIndices_;
    std::vector<int> ParentKeyFieldIndices_;

    int RegisterField(const TDBField* field);
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EAttributeFetchMethod,
    (Composite)
    (ExpressionBuilder)
    (ValueGetter)
);

////////////////////////////////////////////////////////////////////////////////

EAttributeFetchMethod GetFetchMethod(
    const TAttributeSchema* attribute,
    bool emptyPath,
    IQueryContext* context,
    bool readUncommittedChanges = false);

EAttributeFetchMethod GetFetchMethod(
    const TAttributeSchema* attribute,
    bool emptyPath,
    TFetcherContext* context);

////////////////////////////////////////////////////////////////////////////////

struct IFetcher
    : public TRefCounted
{
    virtual void Prefetch(NTableClient::TUnversionedRow row, TObject* object) = 0;
    virtual void Fetch(NTableClient::TUnversionedRow row, TObject* object, NYson::IYsonConsumer* consumer) = 0;
    virtual ~IFetcher() = default;
};

DEFINE_REFCOUNTED_TYPE(IFetcher)

////////////////////////////////////////////////////////////////////////////////

struct ITimestampFetcher
    : public TRefCounted
{
    virtual void Prefetch(NTableClient::TUnversionedRow /*row*/)
    { }
    virtual TTimestamp Fetch(NTableClient::TUnversionedRow row) = 0;
    virtual ~ITimestampFetcher() = default;
};

DEFINE_REFCOUNTED_TYPE(ITimestampFetcher)

////////////////////////////////////////////////////////////////////////////////

NYson::TYsonString FetchYsonString(IFetcherPtr fetcher, TObject* object, NTableClient::TUnversionedRow row);

////////////////////////////////////////////////////////////////////////////////

NQueryClient::NAst::TExpressionPtr TryMakeScalarExpression(
    IFetcherPtr fetcher,
    bool wrapEvaluated,
    TStringBuf forWhat);

////////////////////////////////////////////////////////////////////////////////

IFetcherPtr MakeDbFieldFetcher(TFetcherContext* context, TDBFieldRef dbFieldRef);

IFetcherPtr MakeSelectorFetcher(
    const TResolveAttributeResult& resolveResult,
    TFetcherContext* context);

IFetcherPtr MakeRootFetcher(
    const std::vector<TResolveAttributeResult>& resolvedAttributes,
    TFetcherContext* context,
    EFetchRootOptimizationLevel level);

IFetcherPtr MakeObjectExpressionFetcher(
    const TString& objectExpression,
    TAttributeSchemaCallback permissionCollector,
    TAttributeSchemaCallback readPathsCollector,
    IObjectTypeHandler* typeHandler,
    TFetcherContext* context);

////////////////////////////////////////////////////////////////////////////////

ITimestampFetcherPtr MakeTimestampFetcher(
    const TResolveAttributeResult& resolveResult,
    TFetcherContext* context,
    bool versionedSelectEnabled);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
