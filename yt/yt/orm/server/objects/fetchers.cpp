#include "fetchers.h"

#include "attribute_schema.h"
#include "object_manager.h"
#include "object_reflection.h"
#include "private.h"
#include "query_executor_helpers.h"
#include "transaction.h"
#include "type_handler.h"

#include <yt/yt/orm/library/attributes/merge_attributes.h>

#include <yt/yt/orm/library/query/query_rewriter.h>

#include <yt/yt/orm/library/mpl/projection.h>

#include <yt/yt/library/query/base/query_common.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <util/generic/map.h>

#include <functional>

namespace NYT::NOrm::NServer::NObjects {

using namespace NYT::NYPath;
using namespace NYT::NYson;
using namespace NYT::NYTree;
using namespace NYT::NQueryClient::NAst;
using namespace NYT::NTableClient;

using namespace NOrm::NQuery;

////////////////////////////////////////////////////////////////////////////////

TFetcherContext::TFetcherContext(
    IQueryContext* queryContext,
    TTransaction* transaction,
    bool forceReadUncommittedChanges)
    : QueryContext_(queryContext)
    , Transaction_(transaction)
    , ForceReadUncommittedChanges_(forceReadUncommittedChanges)
{ }

void TFetcherContext::RegisterKeyAndParentKeyFields(IObjectTypeHandler* typeHandler)
{
    KeyFieldIndices_.reserve(typeHandler->GetKeyFields().size());
    for (const auto* field : typeHandler->GetKeyFields()) {
        KeyFieldIndices_.push_back(RegisterField(field));
    }

    ParentKeyFieldIndices_.reserve(typeHandler->GetParentKeyFields().size());
    for (const auto* field : typeHandler->GetParentKeyFields()) {
        ParentKeyFieldIndices_.push_back(RegisterField(field));
    }
}

int TFetcherContext::RegisterField(const TDBField* field)
{
    SelectExprs_.push_back(QueryContext_->GetFieldExpression(field));
    return std::ssize(SelectExprs_) - 1;
}

void TFetcherContext::AddSelectExpression(TExpressionPtr expr)
{
    SelectExprs_.push_back(std::move(expr));
}

const TExpressionList& TFetcherContext::GetSelectExpressions() const
{
    return SelectExprs_;
}

std::optional<std::vector<const TDBField*>> TFetcherContext::ExtractLookupFields(const TDBTable* primaryTable) const
{
    std::vector<const TDBField*> result;
    result.reserve(SelectExprs_.size());

    for (const auto& expr : SelectExprs_) {
        auto* refExpr = expr->As<TReferenceExpression>();
        if (!refExpr || refExpr->Reference.TableName != PrimaryTableAlias ||
            !refExpr->Reference.CompositeTypeAccessor.IsEmpty())
        {
            return std::nullopt;
        }
        auto* field = primaryTable->GetField(refExpr->Reference.ColumnName);
        if (!field) {
            return std::nullopt;
        }
        result.push_back(field);
    }
    return result;
}

TObjectKey TFetcherContext::GetObjectKey(TUnversionedRow row) const
{
    TObjectKey::TKeyFields keyFields;
    keyFields.reserve(KeyFieldIndices_.size());
    for (auto index : KeyFieldIndices_) {
        keyFields.push_back(UnversionedValueToKeyField(row[index]));
    }
    return TObjectKey(std::move(keyFields));
}

TObjectKey TFetcherContext::GetParentKey(TUnversionedRow row) const
{
    TObjectKey::TKeyFields keyFields;
    keyFields.reserve(ParentKeyFieldIndices_.size());
    for (auto index : ParentKeyFieldIndices_) {
        keyFields.push_back(UnversionedValueToKeyField(row[index]));
    }
    return TObjectKey(std::move(keyFields));
}

TObject* TFetcherContext::GetObject(
    TUnversionedRow row) const
{
    auto* typeHandler = QueryContext_->GetTypeHandler();
    return Transaction_->GetObject(
        typeHandler->GetType(),
        GetObjectKey(row),
        GetParentKey(row));
}

std::vector<TObject*> TFetcherContext::GetObjects(
    TRange<TUnversionedRow> rows) const
{
    std::vector<TObject*> objects;
    objects.reserve(rows.size());
    for (auto row : rows) {
        objects.push_back(GetObject(row));
    }
    return objects;
}

////////////////////////////////////////////////////////////////////////////////

struct IScalarFetcher
{
    virtual NQueryClient::NAst::TExpressionPtr MakeScalarExpression(bool wrapEvaluated) = 0;
    virtual ~IScalarFetcher() = default;
};

////////////////////////////////////////////////////////////////////////////////

NYson::TYsonString FetchYsonString(IFetcherPtr fetcher, TObject* object, TUnversionedRow row)
{
    TYsonStringBuilder builder;
    fetcher->Fetch(row, object, builder.GetConsumer());
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

NQueryClient::NAst::TExpressionPtr TryMakeScalarExpression(
    IFetcherPtr fetcher,
    bool wrapEvaluated,
    TStringBuf where)
{
    auto scalarFetcher = dynamic_cast<IScalarFetcher*>(fetcher.Get());
    THROW_ERROR_EXCEPTION_UNLESS(scalarFetcher,
        "Encountered non-scalar expression in %v",
        where);
    return scalarFetcher->MakeScalarExpression(wrapEvaluated);
}

////////////////////////////////////////////////////////////////////////////////

EAttributeFetchMethod GetFetchMethod(
    const TAttributeSchema* attribute,
    bool emptyPath,
    IQueryContext* context,
    bool readUncommittedChanges)
{
    if (const auto* scalarAttribute = attribute->TryAsScalar(); scalarAttribute) {
        if (readUncommittedChanges ||
            (scalarAttribute->IsAnnotationsAttribute() &&
            (emptyPath || HasCompositeOrPolymorphicKey(context->GetTypeHandler()))))
        {
            return EAttributeFetchMethod::ValueGetter;
        }
        if (scalarAttribute->HasExpressionBuilder()) {
            return EAttributeFetchMethod::ExpressionBuilder;
        }
        if (scalarAttribute->HasValueGetter()) {
            return EAttributeFetchMethod::ValueGetter;
        }
    } else {
        return EAttributeFetchMethod::Composite;
    }

    THROW_ERROR_EXCEPTION("Attribute %Qv cannot be fetched",
        attribute->FormatPathEtc());
}

EAttributeFetchMethod GetFetchMethod(
    const TAttributeSchema* attribute,
    bool emptyPath,
    TFetcherContext* context)
{
    return GetFetchMethod(attribute, emptyPath, context->GetQueryContext(), context->ForceReadUncommittedChanges());
}

namespace {

////////////////////////////////////////////////////////////////////////////////

NQueryClient::NAst::TExpressionPtr MakeScalarExpressionValidated(
    IQueryContext* context,
    const TScalarAttributeSchema* attribute,
    const TString& path)
{
    THROW_ERROR_EXCEPTION_UNLESS(attribute->HasExpressionBuilder(),
        "Attribute %Qv lacks expression builder",
        attribute->FormatPathEtc());

    return attribute->RunExpressionBuilder(context, path, EAttributeExpressionContext::Fetch);
}

////////////////////////////////////////////////////////////////////////////////

class TScalarFetcherBase
    : public IScalarFetcher
{
protected:
    int Index_;

    void Initialize(TFetcherContext* context)
    {
        Index_ = context->GetSelectExpressions().size();
        context->AddSelectExpression(MakeScalarExpression(/*wrapEvaluated*/ false));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDBFieldFetcher
    : public IFetcher
    , public TScalarFetcherBase
{
public:
    TDBFieldFetcher(
        TFetcherContext* fetcherContext,
        const TDBFieldRef& field,
        IQueryContext* queryContext)
        : QueryContext_(queryContext)
        , Field_(field)
    {
        Initialize(fetcherContext);
    }

    void Prefetch(TUnversionedRow /*row*/, TObject* /*object*/)
    { }

    void Fetch(TUnversionedRow row, TObject* /*object*/, IYsonConsumer* consumer)
    {
        UnversionedValueToYson(row[Index_], consumer);
    }

    NQueryClient::NAst::TExpressionPtr MakeScalarExpression(bool wrapEvaluated)
    {
        // Only fields as DB expressions are supported for now.
        return QueryContext_->CreateFieldExpression(TDBFieldRef{
                .TableName = Field_.TableName,
                .Name = Field_.Name,
                .Evaluated = Field_.Evaluated,
            },
            wrapEvaluated);
    }

private:
    IQueryContext* const QueryContext_;
    const TDBFieldRef Field_;
};

////////////////////////////////////////////////////////////////////////////////

template <std::derived_from<TAttributeSchema> TSchema>
class TAttributeFetcherBase
    : public IFetcher
{
public:
    TAttributeFetcherBase(
        const TSchema* attribute,
        TFetcherContext* fetcherContext)
        : Attribute_(attribute)
        , FetcherContext_(fetcherContext)
    {
        YT_VERIFY(Attribute_);
    }

protected:
    const TSchema* const Attribute_;
    TFetcherContext* const FetcherContext_;
};

////////////////////////////////////////////////////////////////////////////////

IFetcherPtr MakeAttributeFetcher(
    const TResolveAttributeResult& resolveResult,
    TFetcherContext* fetcherContext);

////////////////////////////////////////////////////////////////////////////////

class TCompositeAttributeFetcher
    : public TAttributeFetcherBase<TCompositeAttributeSchema>
{
public:
    TCompositeAttributeFetcher(
        const TCompositeAttributeSchema* attribute,
        TFetcherContext* fetcherContext)
        : TAttributeFetcherBase(attribute, fetcherContext)
    {
        Initialize();
    }

    void Prefetch(TUnversionedRow row, TObject* object) override
    {
        Attribute_->ForEachImmediateChild([this, row, object] (const TAttributeSchema* schema) {
            if (schema->IsOpaque() || schema->IsControl()) {
                return;
            }
            auto attributeFetcher = ChildFetchers_[schema];
            attributeFetcher->Prefetch(row, object);
        });
    }

    void Fetch(TUnversionedRow row, TObject* object, NYson::IYsonConsumer* consumer) override
    {
        consumer->OnBeginMap();

        Attribute_->ForEachImmediateChild([this, row, object, consumer] (const TAttributeSchema* schema) {
            if (schema->TryAsScalar() && schema->TryAsScalar()->IsEtc()) {
                TUnwrappingConsumer unwrappingConsumer(consumer);
                ChildFetchers_[schema]->Fetch(row, object, &unwrappingConsumer);
                return;
            }

            if (schema->IsControl() || schema->IsOpaque() && !schema->EmitOpaqueAsEntity()) {
                return;
            }
            consumer->OnKeyedItem(schema->GetName());
            if (schema->IsOpaque()) {
                consumer->OnEntity();
            } else {
                ChildFetchers_[schema]->Fetch(row, object, consumer);
            }
        });
        consumer->OnEndMap();
    }

private:
    THashMap<const TAttributeSchema*, IFetcherPtr> ChildFetchers_;

    void Initialize()
    {
        Attribute_->ForEachImmediateChild([this] (const TAttributeSchema* schema) {
            if (schema->IsOpaque() || schema->IsControl()) {
                return;
            }

            ChildFetchers_[schema] = MakeAttributeFetcher({schema, {}}, FetcherContext_);
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

class TScalarAttributeFetcher
    : public TAttributeFetcherBase<TScalarAttributeSchema>
    , public TScalarFetcherBase
{
public:
    TScalarAttributeFetcher(
        const TResolveAttributeResult& resolveResult,
        TFetcherContext* context)
        : TAttributeFetcherBase(resolveResult.Attribute->TryAsScalar(), context)
        , SuffixPath_(resolveResult.SuffixPath)
    {
        Initialize(context);
    }

    void Prefetch(TUnversionedRow /*row*/, TObject* /*object*/) override
    { }

    void Fetch(TUnversionedRow row, TObject* /*object*/, NYson::IYsonConsumer* consumer) override
    {
        UnversionedValueToYson(row[Index_], consumer);
    }

    NQueryClient::NAst::TExpressionPtr MakeScalarExpression(bool /*wrapEvaluated*/) override
    {
        return MakeScalarExpressionValidated(FetcherContext_->GetQueryContext(), Attribute_, SuffixPath_);
    }

private:
    const TString SuffixPath_;
};

////////////////////////////////////////////////////////////////////////////////

class TEvaluatedAttributeFetcher
    : public TAttributeFetcherBase<TScalarAttributeSchema>
{
public:
    TEvaluatedAttributeFetcher(
        const TResolveAttributeResult& resolveResult,
        TFetcherContext* context)
        : TAttributeFetcherBase(resolveResult.Attribute->TryAsScalar(), context)
        , SuffixPath_(resolveResult.SuffixPath)
    { }

    void Prefetch(TUnversionedRow row, TObject* object) override
    {
        if (!object) {
            YT_VERIFY(row);
            object = FetcherContext_->GetObject(row);
        }

        if (Attribute_->HasPreloader()) {
            Attribute_->RunPreloader(object, SuffixPath_);
        }
    }

    void Fetch(TUnversionedRow row, TObject* object, NYson::IYsonConsumer* consumer) override
    {
        if (!object) {
            YT_VERIFY(row);
            object = FetcherContext_->GetObject(row);
        }
        Attribute_->RunValueGetter(FetcherContext_->GetTransaction(), object, consumer, SuffixPath_);
    }
private:
    const TString SuffixPath_;
};

////////////////////////////////////////////////////////////////////////////////

class TExpressionFetcher
    : public IFetcher
    , public TScalarFetcherBase
{
public:
    TExpressionFetcher(
        const TString& expression,
        TFetcherContext* context,
        IObjectTypeHandler* typeHandler,
        TAttributeSchemaCallback permissionCollector,
        TAttributeSchemaCallback readPathsCollector)
        : RawExpression_(expression)
        , FetcherContext_(context)
        , TypeHandler_(typeHandler)
        , PermissionCollector_(std::move(permissionCollector))
        , ReadPathsCollector_(std::move(readPathsCollector))
    {
        Initialize(context);
    }

    NQueryClient::NAst::TExpressionPtr MakeScalarExpression(bool /*wrapEvaluated*/) override
    {
        auto parsedQuery = NQueryClient::ParseSource(RawExpression_, NQueryClient::EParseMode::Expression);
        FetcherContext_->GetQueryContext()->Merge(std::move(parsedQuery->AstHead));
        auto expression = std::get<TExpressionPtr>(parsedQuery->AstHead.Ast);
        auto referenceMapping = [this] (const TReference& ref) {
            THROW_ERROR_EXCEPTION_IF(ref.TableName,
                "Table references are not supported");
            auto attributePath = ref.ColumnName;
            auto [attribute, suffixPath] = ResolveAttribute(TypeHandler_, attributePath, PermissionCollector_);
            const auto* scalarAttribute = attribute->TryAsScalar();
            THROW_ERROR_EXCEPTION_UNLESS(scalarAttribute,
                "Could not reference non-scalar attribute %v in scalar expression",
                attribute->FormatPathEtc());
            ReadPathsCollector_(attribute);
                return MakeScalarExpressionValidated(FetcherContext_->GetQueryContext(), scalarAttribute, suffixPath);
        };

        auto rewriter = TQueryRewriter(FetcherContext_->GetQueryContext(), std::move(referenceMapping));
        return rewriter.Run(expression);
    }

    void Prefetch(TUnversionedRow /*row*/, TObject* /*object*/) override
    { }

    void Fetch(TUnversionedRow row, TObject* /*object*/, IYsonConsumer* consumer) override
    {
        UnversionedValueToYson(row[Index_], consumer);
    }

private:
    const TString RawExpression_;
    TFetcherContext* const FetcherContext_;
    IObjectTypeHandler* const TypeHandler_;
    const TAttributeSchemaCallback PermissionCollector_;
    const TAttributeSchemaCallback ReadPathsCollector_;
};

////////////////////////////////////////////////////////////////////////////////

class TLegacyMergedFetcher
    : public IFetcher
{
public:
    TLegacyMergedFetcher(
        TFetcherContext* context,
        bool fetchFromRoot,
        std::vector<TResolveAttributeResult> resolvedAttributes)
        : FetchFromRoot_(fetchFromRoot)
        , ResolveResults_(std::move(resolvedAttributes))
    {
        Fetchers_.reserve(ResolveResults_.size());
        for (const auto& attribute : ResolveResults_) {
            Fetchers_.push_back(MakeAttributeFetcher(attribute, context));
            YT_VERIFY(attribute.SuffixPath.Empty() || attribute.SuffixPath.StartsWith("/"));
        }
    }

    void Prefetch(TUnversionedRow row, TObject* object) override
    {
        for (auto& fetcher : Fetchers_) {
            fetcher->Prefetch(row, object);
        }
    }

    void Fetch(TUnversionedRow row, TObject* object, NYson::IYsonConsumer* consumer) override
    {
        std::vector<NAttributes::TAttributeValue> attributeValues;
        attributeValues.reserve(Fetchers_.size());
        for (int i = 0; i < std::ssize(Fetchers_); ++i) {
            attributeValues.push_back({
                .Path = (FetchFromRoot_ ? ResolveResults_[i].Attribute->GetPath() : "") + ResolveResults_[i].SuffixPath,
                .Value = FetchYsonString(Fetchers_[i], object, row)});
        }
        auto ysonString = NAttributes::MergeAttributes(std::move(attributeValues));
        consumer->OnRaw(ysonString);
    }

private:
    const bool FetchFromRoot_;
    const std::vector<TResolveAttributeResult> ResolveResults_;
    std::vector<IFetcherPtr> Fetchers_;
};

IFetcherPtr MakeMergedAttributeFetcher(
    TFetcherContext* context,
    const TAttributeSchema* schema,
    const std::vector<NYPath::TYPath>& paths)
{
    std::vector<TResolveAttributeResult> resolvedAttributes;
    resolvedAttributes.reserve(paths.size());
    for (const auto& path : paths) {
        resolvedAttributes.push_back(TResolveAttributeResult{schema, path});
    }
    return New<TLegacyMergedFetcher>(context, /*fetchFromRoot*/ false, std::move(resolvedAttributes));
}

////////////////////////////////////////////////////////////////////////////////

class TOpaqueFetcher
    : public IFetcher
{
public:
    void Prefetch(TUnversionedRow /*row*/, TObject* /*object*/) override
    { }

    void Fetch(TUnversionedRow /*row*/, TObject* /*object*/, NYson::IYsonConsumer* consumer) override
    {
        consumer->OnEntity();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, std::invocable<TKey> TPathProj, std::invocable<TKey> TIsEtcProj, class TFetchersMap>
class TOptimizedMergedFetcher
    : public IFetcher
{
public:
    TOptimizedMergedFetcher(
        TFetchersMap fetchers,
        TPathProj pathProj,
        TIsEtcProj etcProj)
        : Fetchers_(std::move(fetchers))
        , PathProj_(std::move(pathProj))
        , IsEtcProj_(std::move(etcProj))
    {
        NAttributes::ValidateSortedPaths(
            std::ranges::views::keys(Fetchers_),
            PathProj_,
            IsEtcProj_);
    }

    void Prefetch(TUnversionedRow row, TObject* object) override
    {
        for (auto& [_, fetcher] : Fetchers_) {
            fetcher->Prefetch(row, object);
        }
    }

    void Fetch(TUnversionedRow row, TObject* object, NYson::IYsonConsumer* consumer) override
    {
        NAttributes::TMergeAttributesHelper mergeAttributesHelper(consumer);
        for (auto& [key, fetcher] : Fetchers_) {
            bool isEtc = std::invoke(IsEtcProj_, key);
            mergeAttributesHelper.ToNextPath(std::invoke(PathProj_, key), isEtc);
            if (isEtc) {
                TUnwrappingConsumer unwrappingConsumer(consumer);
                fetcher->Fetch(row, object, &unwrappingConsumer);
            } else {
                fetcher->Fetch(row, object, consumer);
            }
        }

        mergeAttributesHelper.Finalize();
    }

private:
    const TFetchersMap Fetchers_;
    TPathProj PathProj_;
    TIsEtcProj IsEtcProj_;
};

template <class TKey, std::invocable<TKey> TPathProj, std::invocable<TKey> TIsEtcProj, class TMap>
IFetcherPtr MakeOptimizedMergedFetcher(TMap fetchers, TPathProj pathProj, TIsEtcProj etcProj)
{
    return New<TOptimizedMergedFetcher<TKey, TPathProj, TIsEtcProj, TMap>>(std::move(fetchers), pathProj, etcProj);
}

void ForEachFetchingLeafChild(const TCompositeAttributeSchema* schema, const TAttributeSchemaCallback& callback)
{
    schema->ForEachImmediateChild([&callback] (const TAttributeSchema* schema) {
        if (schema->IsOpaque()) {
            callback(schema);
            return;
        }

        if (schema->IsControl()) {
            return;
        }

        if (auto* compositeSchema = schema->TryAsComposite()) {
            ForEachFetchingLeafChild(compositeSchema, callback);
        } else {
            callback(schema);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EScalarTimestampFetchMethod,
    (VersionedSelect)
    (TimestampGetter)
    (Null)
);

////////////////////////////////////////////////////////////////////////////////

EScalarTimestampFetchMethod GetScalarTimestampFetchMethod(
    const TScalarAttributeSchema* scalarSchema,
    bool versionedSelectEnabled)
{
    if (scalarSchema->HasTimestampExpressionBuilder() && versionedSelectEnabled) {
        return EScalarTimestampFetchMethod::VersionedSelect;
    } else if (scalarSchema->HasTimestampGetter()) {
        return EScalarTimestampFetchMethod::TimestampGetter;
    } else {
        return EScalarTimestampFetchMethod::Null;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TScalarTimestampFetcher
    : public ITimestampFetcher
    , public TScalarFetcherBase
{
public:
    TScalarTimestampFetcher(const TResolveAttributeResult& resolveResult, TFetcherContext* fetcherContext)
        : ResolveResult_(resolveResult)
        , QueryContext_(fetcherContext->GetQueryContext())
    {
        Initialize(fetcherContext);
        fetcherContext->VersionedIOMode() = EVersionedIOMode::LatestTimestamp;
    }

    NQueryClient::NAst::TExpressionPtr MakeScalarExpression(bool /*wrapEvaluated*/) override
    {
        return ResolveResult_.Attribute->AsScalar()->RunTimestampExpressionBuilder(
            QueryContext_,
            ResolveResult_.SuffixPath);
    }

    TTimestamp Fetch(TUnversionedRow row) override
    {
        YT_VERIFY(row[Index_].Type == EValueType::Uint64);
        return row[Index_].Data.Uint64;
    }

private:
    const TResolveAttributeResult& ResolveResult_;
    IQueryContext* const QueryContext_;
};

////////////////////////////////////////////////////////////////////////////////

class TNullTimestampFetcher
    : public ITimestampFetcher
{
public:
    TTimestamp Fetch(TUnversionedRow /*row*/) override
    {
        return TTimestamp{0};
    }
};

////////////////////////////////////////////////////////////////////////////////

class TEvaluatedTimestampFetcher
    : public ITimestampFetcher
{
public:
    TEvaluatedTimestampFetcher(const TResolveAttributeResult& resolveResult, TFetcherContext* fetcherContext)
        : ResolveResult_(resolveResult)
        , Attribute_(resolveResult.Attribute->AsScalar())
        , FetcherContext_(fetcherContext)
    { }

    void Prefetch(TUnversionedRow row) override
    {
        if (Attribute_->HasTimestampPregetter()) {
            auto* object = FetcherContext_->GetObject(row);
            Attribute_->RunTimestampPregetter(object, ResolveResult_.SuffixPath);
        }
    }

    TTimestamp Fetch(TUnversionedRow row) override
    {
        YT_VERIFY(Attribute_->HasTimestampGetter());
        auto* object = FetcherContext_->GetObject(row);
        return Attribute_->RunTimestampGetter(object, ResolveResult_.SuffixPath);
    }

private:
    const TResolveAttributeResult ResolveResult_;

    // TODO(grigminakov): Get rid of excessive field in YTORM-1081.
    const TScalarAttributeSchema* Attribute_;
    const TFetcherContext* FetcherContext_;
};

////////////////////////////////////////////////////////////////////////////////

ITimestampFetcherPtr MakeScalarTimestampFetcher(
    TFetcherContext* fetcherContext,
    const TScalarAttributeSchema* schema,
    const TString& suffixPath,
    bool versionedSelectEnabled)
{
    switch (GetScalarTimestampFetchMethod(schema, versionedSelectEnabled)) {
        case EScalarTimestampFetchMethod::VersionedSelect:
            return New<TScalarTimestampFetcher>(TResolveAttributeResult{schema, suffixPath}, fetcherContext);
        case EScalarTimestampFetchMethod::TimestampGetter:
            return New<TEvaluatedTimestampFetcher>(TResolveAttributeResult{schema, suffixPath}, fetcherContext);
        case EScalarTimestampFetchMethod::Null:
            return New<TNullTimestampFetcher>();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TCompositeTimestampFetcher
    : public ITimestampFetcher
{
public:
    TCompositeTimestampFetcher(
        const TCompositeAttributeSchema* schema,
        TFetcherContext* fetcherContext,
        bool versionedSelectEnabled)
    {
        schema->ForEachLeafAttribute([&] (const TScalarAttributeSchema* leaf) {
            if (leaf->IsView()) {
                return false;
            }

            Fetchers_.push_back(MakeScalarTimestampFetcher(
                fetcherContext,
                leaf,
                /*suffixPath*/ {},
                versionedSelectEnabled));
            return false;
        });
    }

    void Prefetch(TUnversionedRow row) override
    {
        for (const auto& fetcher : Fetchers_) {
            fetcher->Prefetch(row);
        }
    }

    TTimestamp Fetch(TUnversionedRow row) override
    {
        TTimestamp result = 0;
        for (const auto& fetcher : Fetchers_) {
            result = std::max(result, fetcher->Fetch(row));
        }
        return result;
    }

private:
    std::vector<ITimestampFetcherPtr> Fetchers_;
};

////////////////////////////////////////////////////////////////////////////////

IFetcherPtr MakeSquashedFetcher(
    TFetcherContext* context,
    const TAttributeSchema* schema,
    std::vector<NYPath::TYPath>& paths)
{
    NAttributes::SortAndRemoveNestedPaths(paths);
    auto leafFetchers = NMpl::MakeProjectedMultiMap<NYPath::TYPath, IFetcherPtr>(std::identity{});
    for (const auto& path : paths) {
        leafFetchers.emplace(path, MakeAttributeFetcher(TResolveAttributeResult{schema, path}, context));
    }

    return MakeOptimizedMergedFetcher<NYPath::TYPath>(
        leafFetchers,
        std::identity{},
        NMpl::TConstantProjection<bool, false>{});
}

IFetcherPtr MakeOptimizedRootFetcher(
    TFetcherContext* context,
    const std::vector<TResolveAttributeResult>& resolvedAttributes,
    EFetchRootOptimizationLevel level)
{
    YT_VERIFY(level >= EFetchRootOptimizationLevel::SchemaOnly);
    std::map<const TScalarAttributeSchema*, std::vector<NYPath::TYPath>> groupedSuffixes;
    std::vector<const TAttributeSchema*> opaqueSchemas;
    for (const auto& resolvedAttribute : resolvedAttributes) {
        if (auto* compositeSchema = resolvedAttribute.Attribute->TryAsComposite()) {
            ForEachFetchingLeafChild(
                compositeSchema,
                [&groupedSuffixes, &opaqueSchemas] (const TAttributeSchema* schema)
            {
                if (schema->IsOpaque()) {
                    if (schema->EmitOpaqueAsEntity()) {
                        opaqueSchemas.push_back(schema);
                    }
                } else {
                    auto* scalarSchema = schema->TryAsScalar();
                    YT_VERIFY(scalarSchema);
                    groupedSuffixes[scalarSchema].push_back(NYPath::TYPath{});
                }
            });
        } else {
            groupedSuffixes[resolvedAttribute.Attribute->TryAsScalar()].push_back(resolvedAttribute.SuffixPath);
        }
    }

    auto fetchers = NMpl::MakeProjectedMultiMap<const TAttributeSchema*, IFetcherPtr>(
        [] (const TAttributeSchema* schema) { return schema->GetPath(); });
    for (auto& [schema, pathGroup] : groupedSuffixes) {
        bool hasEmptyPath = std::ranges::any_of(pathGroup, &NYPath::TYPath::Empty);
        auto fetchMethod = GetFetchMethod(schema, hasEmptyPath, context);
        if ((fetchMethod == EAttributeFetchMethod::ExpressionBuilder &&
            level >= EFetchRootOptimizationLevel::SquashExpressionBuilders) ||
            (fetchMethod == EAttributeFetchMethod::ValueGetter &&
            level >= EFetchRootOptimizationLevel::SquashValueGetters &&
            schema->GetValueGetterType() == EValueGetterType::Default))
        {
            if (hasEmptyPath) {
                fetchers.emplace(schema, MakeAttributeFetcher(
                    TResolveAttributeResult{schema, NYPath::TYPath{}}, context));
            } else {
                fetchers.emplace(schema, MakeSquashedFetcher(context, schema, pathGroup));
            }
        } else {
            fetchers.emplace(schema, MakeMergedAttributeFetcher(context, schema, pathGroup));
        }
    }

    for (const auto& schema : opaqueSchemas) {
        auto nextSchemaIterator = fetchers.lower_bound(schema);
        if (nextSchemaIterator == fetchers.end() ||
            !HasPrefix(nextSchemaIterator->first->GetPath(), schema->GetPath()))
        {
            fetchers.emplace(schema, New<TOpaqueFetcher>());
        }
    }

    return MakeOptimizedMergedFetcher<const TScalarAttributeSchema*>(
        std::move(fetchers),
        &TAttributeSchema::GetPath,
        [] (const TAttributeSchema* schema) { return schema->TryAsScalar() && schema->TryAsScalar()->IsEtc(); });
}

////////////////////////////////////////////////////////////////////////////////

IFetcherPtr MakeAttributeFetcher(
    const TResolveAttributeResult& resolveResult,
    TFetcherContext* fetcherContext)
{
    switch (GetFetchMethod(resolveResult.Attribute, /*emptyPath*/ resolveResult.SuffixPath.Empty(), fetcherContext)) {
        case EAttributeFetchMethod::Composite: {
            auto* compositeAttribute = resolveResult.Attribute->TryAsComposite();
            YT_VERIFY(compositeAttribute);
            return New<TCompositeAttributeFetcher>(compositeAttribute, fetcherContext);
        }
        case EAttributeFetchMethod::ExpressionBuilder:
            return New<TScalarAttributeFetcher>(resolveResult, fetcherContext);
        case EAttributeFetchMethod::ValueGetter:
            return New<TEvaluatedAttributeFetcher>(resolveResult, fetcherContext);
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

TString CompatAttributeExpression(const TString& attributePath)
{
    if (attributePath.size() > 0 && attributePath[0] == '/') {

        return FormatReference(TReference(attributePath));
    }

    return attributePath;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

IFetcherPtr MakeDbFieldFetcher(TFetcherContext* context, TDBFieldRef dbFieldRef)
{
    return New<TDBFieldFetcher>(context, std::move(dbFieldRef), context->GetQueryContext());
}

IFetcherPtr MakeSelectorFetcher(
    const TResolveAttributeResult& resolveResult,
    TFetcherContext* context)
{
    return MakeAttributeFetcher(resolveResult, context);
}

IFetcherPtr MakeRootFetcher(
    const std::vector<TResolveAttributeResult>& resolvedAttributes,
    TFetcherContext* context,
    EFetchRootOptimizationLevel level)
{
    if (level == EFetchRootOptimizationLevel::None) {
        return New<TLegacyMergedFetcher>(context, /*fetchFromRoot*/ true, resolvedAttributes);
    } else {
        return MakeOptimizedRootFetcher(context, resolvedAttributes, level);
    }
}

IFetcherPtr MakeObjectExpressionFetcher(
    const TString& objectExpression,
    TAttributeSchemaCallback permissionCollector,
    TAttributeSchemaCallback readPathsCollector,
    IObjectTypeHandler* typeHandler,
    TFetcherContext* context)
{
    return New<TExpressionFetcher>(
        CompatAttributeExpression(objectExpression),
        context,
        typeHandler,
        std::move(permissionCollector),
        std::move(readPathsCollector));
}

////////////////////////////////////////////////////////////////////////////////

ITimestampFetcherPtr MakeTimestampFetcher(
    const TResolveAttributeResult& resolveResult,
    TFetcherContext* context,
    bool versionedSelectEnabled)
{
    if (auto* compositeAttributeSchema = resolveResult.Attribute->TryAsComposite()) {
        return New<TCompositeTimestampFetcher>(compositeAttributeSchema, context, versionedSelectEnabled);
    } else {
        return MakeScalarTimestampFetcher(
            context,
            resolveResult.Attribute->AsScalar(),
            resolveResult.SuffixPath,
            versionedSelectEnabled);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
