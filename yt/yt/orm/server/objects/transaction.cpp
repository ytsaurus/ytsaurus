#include "transaction.h"

#include "attribute_matcher.h"
#include "attribute_schema.h"
#include "config.h"
#include "db_schema.h"
#include "db_config.h"
#include "fetchers.h"
#include "helpers.h"
#include "history_manager.h"
#include "key_util.h"
#include "object_manager.h"
#include "object_reflection.h"
#include "persistence.h"
#include "private.h"
#include "transaction_manager.h"
#include "type_handler.h"
#include "query_executor_helpers.h"

#include <yt/yt/orm/server/access_control/access_control_manager.h>
#include <yt/yt/orm/server/access_control/helpers.h>

#include <yt/yt/orm/server/master/bootstrap.h>
#include <yt/yt/orm/server/master/config.h>
#include <yt/yt/orm/server/master/yt_connector.h>

#include <yt/yt/orm/server/objects/proto/history_event_etc.pb.h>

#include <yt/yt_proto/yt/orm/data_model/generic.pb.h>

#include <yt/yt/orm/client/objects/object_filter.h>
#include <yt/yt/orm/client/objects/registry.h>

#include <yt/yt/orm/library/query/filter_introspection.h>
#include <yt/yt/orm/library/query/query_optimizer.h>
#include <yt/yt/orm/library/query/type_inference.h>

#include <yt/yt/library/query/base/ast.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/table_client/config.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/api/rpc_proxy/transaction_impl.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/misc/mpl.h>
#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/ypath_resolver.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/guid.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/yt/small_containers/compact_flat_map.h>
#include <library/cpp/yt/small_containers/compact_set.h>

#include <library/cpp/protobuf/interop/cast.h>

namespace NYT::NOrm::NServer::NObjects {

using namespace NAccessControl;

using namespace NClient::NObjects;

using namespace NYT::NApi;
using namespace NYT::NConcurrency;
using namespace NYT::NLogging;
using namespace NYT::NMpl;
using namespace NYT::NNet;
using namespace NYT::NQueryClient::NAst;
namespace NQlAst = NYT::NQueryClient::NAst;
using namespace NYT::NTableClient;
using namespace NYT::NYPath;
using namespace NYT::NYTree;
using namespace NYT::NYson;
using namespace NYT::NTracing;

using NYT::NQueryClient::TSourceLocation;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr NYPath::TYPathBuf AddChildAccessControlPath("/access/children");
constexpr NYPath::TYPathBuf RemoveObjectAccessControlPath("/access/lifetime");
constexpr NYPath::TYPathBuf AclAttributePath("/meta/acl");
constexpr NYPath::TYPathBuf FinalizersAttributePath("/meta/finalizers");
constexpr NYPath::TYPathBuf FinalizationTimeAttributePath("/meta/finalization_start_time");
constexpr NYPath::TYPathBuf AddFinalizerControlPath("/control/add_finalizer");
constexpr NYPath::TYPathBuf CompleteFinalizerControlPath("/control/complete_finalization");

////////////////////////////////////////////////////////////////////////////////

class TExtendedNameTable
{
public:
    explicit TExtendedNameTable(const TDBTable* table) {
        Underlying_ = New<TNameTable>();

        const auto& tableKeyFields = table->GetKeyFields(/*filterEvaluatedFields*/ true);
        IdToField_.reserve(tableKeyFields.size());
        for (int index = 0; index < std::ssize(tableKeyFields); ++index) {
            YT_VERIFY(index == Register(tableKeyFields[index]));
        }
    }

    const TDBField* GetFieldById(int id) const
    {
        return GetOrCrash(IdToField_, id);
    }

    int GetIdByField(const TDBField* field) const
    {
        return Underlying_->GetId(field->Name);
    }

    int GetIdByFieldOrRegister(const TDBField* field)
    {
        auto id = Underlying_->GetIdOrRegisterName(field->Name);
        auto it = IdToField_.emplace(id, field).first;
        YT_VERIFY(it->second == field);
        return id;
    }

    int GetSize() const
    {
        return Underlying_->GetSize();
    }

    TNameTablePtr GetUnderlying() const
    {
        return Underlying_;
    }

private:
    TNameTablePtr Underlying_;
    THashMap<int, const TDBField*> IdToField_;

    int Register(const TDBField* field)
    {
        auto id = Underlying_->RegisterName(field->Name);
        auto it = IdToField_.emplace(id, field).first;
        YT_VERIFY(it->second == field);
        return id;
    }
};

////////////////////////////////////////////////////////////////////////////////

bool CommitStartTimestampRequired(const THistoryIndexTable& table)
{
    return table.CommitTime == EHistoryCommitTime::TransactionCommitStart &&
        table.TimeMode == EHistoryTimeMode::Logical;
}

bool CommitStartTimestampRequired(const IHistoryManager* historyManager)
{
    if (auto* primaryTable = historyManager->GetPrimaryWriteTable();
        CommitStartTimestampRequired(*primaryTable))
    {
        return true;
    }
    if (auto* secondaryTable = historyManager->GetSecondaryWriteTable()) {
        return CommitStartTimestampRequired(*secondaryTable);
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

std::vector<int> GetNonZeroBits(const TDynBitMap& bitmap)
{
    std::vector<int> result;
    result.reserve(bitmap.Count());
    for (auto bit = bitmap.FirstNonZeroBit();
        bit < bitmap.Size();
        bit = bitmap.NextNonZeroBit(bit))
    {
        result.push_back(bit);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

template<class T, template<bool Arg> class TExpected>
concept CWithAnyBoolArg = std::same_as<T, TExpected<true>> || std::same_as<T, TExpected<false>>;

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

void Serialize(TVersionedValue* value, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginAttributes()
            .Item("timestamp").Value(value->Timestamp)
        .EndAttributes()
        .Value(value);
}

void ConvertVersionedRowTimestampsToFluentMap(TFluentMap attributes, const TVersionedRow row)
{
    attributes
        .Item("write_timestamps").BeginList()
            .DoFor(
                row.BeginWriteTimestamps(),
                row.EndWriteTimestamps(),
                [&] (TFluentList fluent, const TTimestamp* timestamp) {
                    fluent.Item().Value(*timestamp);
                })
        .EndList()
        .Item("delete_timestamps").BeginList()
            .DoFor(
                row.BeginDeleteTimestamps(),
                row.EndDeleteTimestamps(),
                [&] (TFluentList fluent, const TTimestamp* timestamp) {
                    fluent.Item().Value(*timestamp);
                })
        .EndList();
}

TYsonString ConvertVersionedRowTimestampsToYsonString(
    const TVersionedRow row)
{
    return BuildYsonStringFluently(EYsonFormat::Text)
        .BeginAttributes()
            .Do([&] (TFluentMap attributes) {
                ConvertVersionedRowTimestampsToFluentMap(attributes, row);
            })
        .EndAttributes()
        .BeginList()
            .Item().Entity()
        .EndList();
}

template <class TValueFilter>
TYsonString ConvertVersionedRowToYsonString(
    const TVersionedRow row,
    TValueFilter&& valueFilter)
{
    return BuildYsonStringFluently(EYsonFormat::Text)
        .BeginAttributes()
            .Do([&] (TFluentMap attributes) {
                ConvertVersionedRowTimestampsToFluentMap(attributes, row);
            })
        .EndAttributes()
        .BeginList()
            .DoFor(row.BeginKeys(), row.EndKeys(), [] (TFluentList fluent, const TUnversionedValue* value) {
                fluent
                    .Item().Value(*value);
            })
            .DoFor(row.BeginValues(), row.EndValues(), [&] (TFluentList fluent, const TVersionedValue* value) {
                if (valueFilter(*value)) {
                    fluent
                        .Item().Value(*value);
                } else {
                    fluent
                        .Item().Entity();
                }
            })
        .EndList();
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TAttributeGroupingExpressions& groupingExpression,
    TStringBuf /*spec*/)
{
    Format(builder, "{Expressions: %v}", groupingExpression.Expressions);
}

void FormatValue(
    TStringBuilderBase* builder,
    const TAttributeAggregateExpressions& groupingExpression,
    TStringBuf /*spec*/)
{
    Format(builder, "{Expressions: %v}", groupingExpression.Expressions);
}

void FormatValue(
    TStringBuilderBase* builder,
    const TIndex& index,
    TStringBuf /*format*/)
{
    builder->AppendFormat("{Name: %v}", index.Name);
}

void FormatValue(
    TStringBuilderBase* builder,
    const TTimeInterval& timeInterval,
    TStringBuf /*format*/)
{
    builder->AppendFormat("{Begin: %v, End: %v}",
        timeInterval.Begin,
        timeInterval.End);
}

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath GetUpdateRequestPath(const TUpdateRequest& updateRequest)
{
    return Visit(updateRequest,
        [] (const auto& request) {
            return request.Path;
        });
}

////////////////////////////////////////////////////////////////////////////////

class TQueryContext
    : public IQueryContext
{
public:
    TQueryContext(
        NMaster::IBootstrap* bootstrap,
        TObjectTypeValue objectType,
        ISession* session,
        const TScalarAttributeIndexDescriptor* indexDescriptor = nullptr,
        bool allowAnnotations = true)
        : Bootstrap_(bootstrap)
        , ObjectType_(objectType)
        , Session_(session)
        , ConfigsSnapshot_(Session_->GetConfigsSnapshot())
        , IndexDescriptor_(indexDescriptor)
        , AllowAnnotations_(allowAnnotations)
        , Query_(MakeQuery())
    {
        if (indexDescriptor) {
            auto indexMode = ConfigsSnapshot_.ObjectManager
                ->TryGetIndexMode(IndexDescriptor_->IndexName)
                .value_or(IndexDescriptor_->Mode);
            THROW_ERROR_EXCEPTION_UNLESS(
                indexMode == EIndexMode::Enabled ||
                indexMode == EIndexMode::Building &&
                ConfigsSnapshot_.ObjectManager->IndexesWithAllowedBuildingRead.contains(IndexDescriptor_->IndexName),
                "Cannot query %v index %Qv",
                indexMode,
                IndexDescriptor_->IndexName);
        }
    }

    TExpressionPtr CreateFieldExpression(const TDBFieldRef& fieldRef, bool wrapEvaluated) override
    {
        TString tableAlias;
        if (fieldRef.TableName == GetTypeHandler()->GetTable()->GetName()) {
            tableAlias = PrimaryTableAlias;
        } else if (GetIndexDescriptor() && fieldRef.TableName == GetIndexDescriptor()->Table->GetName()) {
            tableAlias = IndexTableAlias;
        } else {
            THROW_ERROR_EXCEPTION("Table name %Qv is unknown to the query context", fieldRef.TableName);
        }
        auto expression = New<TReferenceExpression>(
            TSourceLocation(),
            fieldRef.Name,
            tableAlias);

        if (wrapEvaluated && fieldRef.Evaluated) {
            // Wrap DBExpression with type agnostic zero-diff function to force QL infer its value, see YTORM-403.
            return New<TFunctionExpression>(
                TSourceLocation(),
                "IF_NULL",
                TExpressionList{std::move(expression), New<TLiteralExpression>(TSourceLocation(), TNullLiteralValue{})});
        }
        return expression;
    }

    IObjectTypeHandler* GetTypeHandler() override
    {
        return Bootstrap_->GetObjectManager()->GetTypeHandlerOrCrash(ObjectType_);
    }

    TExpressionPtr GetFieldExpression(const TDBField* field, const TYPath& path = TYPath()) override
    {
        return GetFieldExpressionImpl(field, PrimaryTableAlias, path, field->Type);
    }

    TExpressionPtr GetAnnotationExpression(std::string_view name) override
    {
        if (!AllowAnnotations_) {
            THROW_ERROR_EXCEPTION("Accessing /annotations is not allowed for this type of query");
        }
        auto it = AnnotationNameToExpression_.find(name);
        if (it == AnnotationNameToExpression_.end()) {
            auto foreignTableAlias = AnnotationsTableAliasPrefix + ToString(AnnotationNameToExpression_.size());
            const auto& ytConnector = Bootstrap_->GetYTConnector();
            auto annotationsPath = ytConnector->GetTablePath(&AnnotationsTable);
            auto keyFields = GetTypeHandler()->GetKeyFields();
            YT_VERIFY(keyFields.size() == 1); // TODO(deep): YTORM-206, annotations for composite keys
            Query_->Joins.push_back(NQueryClient::NAst::TJoin(
                true,
                TTableDescriptor(annotationsPath, foreignTableAlias),
                TExpressionList{
                    New<TReferenceExpression>(TSourceLocation(), keyFields[0]->Name, PrimaryTableAlias),
                    New<TLiteralExpression>(TSourceLocation(), static_cast<i64>(ObjectType_)),
                    New<TLiteralExpression>(TSourceLocation(), TString(name))},
                TExpressionList{
                    New<TReferenceExpression>(TSourceLocation(), AnnotationsTable.Fields.ObjectId.Name, foreignTableAlias),
                    New<TReferenceExpression>(TSourceLocation(), AnnotationsTable.Fields.ObjectType.Name, foreignTableAlias),
                    New<TReferenceExpression>(TSourceLocation(), AnnotationsTable.Fields.Name.Name, foreignTableAlias)},
                std::nullopt));

            auto expr = New<TReferenceExpression>(TSourceLocation(), AnnotationsTable.Fields.Value.Name, foreignTableAlias);
            it = AnnotationNameToExpression_.emplace(name, std::move(expr)).first;
        }
        return it->second;
    }

    TExpressionPtr GetScalarAttributeExpression(
        const TDBField* field,
        const TYPath& path,
        EAttributeExpressionContext expressionContext,
        NTableClient::EValueType type) override
    {
        const TScalarAttributeIndexDescriptor* indexedDescriptor = GetIndexDescriptor();
        if (!indexedDescriptor ||
            (indexedDescriptor->Repeated && expressionContext == EAttributeExpressionContext::Fetch))
        {
            return GetFieldExpressionImpl(field, PrimaryTableAlias, path, type);
        }
        auto* indexField = indexedDescriptor->GetIndexFieldByAttribute(field, path);
        if (!indexField) {
            return GetFieldExpressionImpl(field, PrimaryTableAlias, path, type);
        }
        // Empty path is intentional here: path has been already considered during index table creation.
        return GetFieldExpressionImpl(indexField, IndexTableAlias, TYPath(), EValueType::Any);
    }

    NQueryClient::NAst::TExpressionPtr GetScalarTimestampExpression(
        const TDBField* field,
        const NYPath::TYPath& /*path*/) override
    {
        return GetTimestampExpressionImpl(field, PrimaryTableAlias);
    }

    const TScalarAttributeIndexDescriptor* GetIndexDescriptor() const override
    {
        return IndexDescriptor_;
    }

    NQlAst::TQuery* GetQuery() const override
    {
        return Query_.get();
    }

    std::string Finalize() override
    {
        YT_VERIFY(!GetQuery()->WherePredicate || GetQuery()->WherePredicate->size() == 1);
        const TScalarAttributeIndexDescriptor* indexedDescriptor = GetIndexDescriptor();
        if (indexedDescriptor) {
            std::vector<TString> indexKeyColumnNames;
            indexKeyColumnNames.reserve(indexedDescriptor->Table->IndexKey.size());
            for (const auto* field : indexedDescriptor->Table->IndexKey) {
                indexKeyColumnNames.push_back(field->Name);
            }
            bool definedIndexAttribute = false;
            TExpression* wherePredicate = nullptr;

            if (GetQuery()->WherePredicate && GetQuery()->WherePredicate->size() == 1) {
                wherePredicate = (*GetQuery()->WherePredicate)[0];
                definedIndexAttribute = NQuery::IntrospectFilterForDefinedReference(
                    wherePredicate,
                    indexKeyColumnNames[0],
                    IndexTableAlias,
                    /*allowValueRange*/ !indexedDescriptor->Repeated);
            }
            if (!definedIndexAttribute) {
                THROW_ERROR_EXCEPTION(NClient::EErrorCode::IndexNotApplicable,
                    "Specified index %Qv is not applicable, "
                    "because it is not used in the query filter, "
                    "which can cause incorrect query result; "
                    "please use at least the first index column %Qv",
                    indexedDescriptor->IndexName,
                    indexKeyColumnNames[0]);
            }

            if (indexedDescriptor->Repeated &&
                !NQuery::TryOptimizeGroupByWithUniquePrefix(
                    wherePredicate,
                    indexKeyColumnNames,
                    IndexTableAlias)) {
                YT_VERIFY(!GetQuery()->GroupExprs.has_value());

                TExpressionList groupByExpressions;
                groupByExpressions.reserve(indexedDescriptor->Table->ObjectTableKey.size());
                for (auto* field : indexedDescriptor->Table->ObjectTableKey) {
                    groupByExpressions.push_back(New<TReferenceExpression>(
                        TSourceLocation(),
                        field->Name,
                        IndexTableAlias));
                }
                GetQuery()->GroupExprs = std::move(groupByExpressions);

                THashSet<TString> aliases;
                for (auto& expr : *GetQuery()->SelectExprs) {
                    auto* refExpr = expr->As<TReferenceExpression>();
                    expr = New<TFunctionExpression>(
                        TSourceLocation{},
                        "FIRST",
                        TExpressionList{expr});
                    if (refExpr && aliases.insert(refExpr->Reference.ColumnName).second) {
                        expr = New<TAliasExpression>(
                            TSourceLocation{},
                            expr,
                            refExpr->Reference.ColumnName);
                    }
                }
                for (auto& orderExpr : GetQuery()->OrderExpressions) {
                    for (auto& expr : orderExpr.first) {
                        if (auto* refExpr = expr->As<TReferenceExpression>()) {
                            // Original reference may contain table name, e.g.: `ORDER BY p.spec.year`,
                            // which does not make sense after applying GROUP BY.
                            //
                            // New reference has no table name specified therefore implicitly references
                            // aliased expression.
                            // e.g.: references spec.year, which is defined as `FIRST(p.spec.year) AS spec.year`.
                            expr = New<TReferenceExpression>(
                                TSourceLocation{},
                                refExpr->Reference.ColumnName);
                        }
                    }
                }
            }
            if (NQuery::TryOptimizeJoin(GetQuery())) {
                YT_LOG_DEBUG("The select query was optimized");
            }
        }

        for (const auto& orderExpr : GetQuery()->OrderExpressions) {
            for (const auto& expr : orderExpr.first) {
                ThrowIfExpressionIsOfTypeAny("Order by", expr);
            }
        }

        if (const auto& groupExprs = GetQuery()->GroupExprs) {
            for (const auto* expr : *groupExprs) {
                ThrowIfExpressionIsOfTypeAny("Group by", expr);
            }
        }

        return FormatQuery(*GetQuery());
    }

private:
    NMaster::IBootstrap* const Bootstrap_;
    const TObjectTypeValue ObjectType_;
    ISession* Session_;
    TTransactionConfigsSnapshot ConfigsSnapshot_;
    const TScalarAttributeIndexDescriptor* IndexDescriptor_;
    const bool AllowAnnotations_;
    const std::unique_ptr<NQlAst::TQuery> Query_;

    THashMap<const TDBField*, TExpressionPtr> FieldToExpression_;
    THashMap<TString, TExpressionPtr> AnnotationNameToExpression_;

    std::unique_ptr<NQlAst::TQuery> MakeQuery()
    {
        auto query = std::make_unique<NQlAst::TQuery>();
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        const auto* table = GetTypeHandler()->GetTable();
        const auto tableDescriptor = TTableDescriptor(ytConnector->GetTablePath(table), PrimaryTableAlias);
        if (GetIndexDescriptor() != nullptr) {
            auto* indexTable = GetIndexDescriptor()->Table;
            query->Table = TTableDescriptor(ytConnector->GetTablePath(indexTable), IndexTableAlias);

            const auto& tableKeyFields = table->GetKeyFields(/*filterEvaluatedFields*/ true);
            YT_VERIFY(indexTable->ObjectTableKey.size() == tableKeyFields.size());
            TExpressionList lhs, rhs;
            lhs.reserve(indexTable->ObjectTableKey.size());
            rhs.reserve(tableKeyFields.size());
            for (const auto* field : indexTable->ObjectTableKey) {
                lhs.push_back(New<TReferenceExpression>(
                    TSourceLocation(),
                    field->Name,
                    IndexTableAlias));
            }
            for (const auto* field : tableKeyFields) {
                rhs.push_back(New<TReferenceExpression>(
                    TSourceLocation(),
                    field->Name,
                    PrimaryTableAlias));
            }
            query->Joins.push_back(NQueryClient::NAst::TJoin(
                false /*isLeft*/,
                tableDescriptor,
                lhs,
                rhs,
                std::nullopt));
        } else {
            query->Table = tableDescriptor;
        }
        query->SelectExprs.emplace();
        return query;
    }

    TExpressionPtr GetFieldExpressionImpl(
        const TDBField* field,
        const TString& tableAlias,
        const TYPath& path,
        NTableClient::EValueType type)
    {
        auto it = FieldToExpression_.find(field);
        if (it == FieldToExpression_.end()) {
            auto expr = New<TReferenceExpression>(TSourceLocation(), field->Name, tableAlias);
            it = FieldToExpression_.emplace(field, std::move(expr)).first;
        }
        auto* expr = it->second;
        if (!path.empty()) {
            expr = New<TFunctionExpression>(
                TSourceLocation(),
                GetYsonExtractFunction(type),
                TExpressionList{
                    std::move(expr),
                    New<TLiteralExpression>(
                        TSourceLocation(),
                        path)
                });
        }
        return expr;
    }

    TExpressionPtr GetTimestampExpressionImpl(
        const TDBField* field,
        const TString& tableAlias)
    {
        return New<TReferenceExpression>(
            TSourceLocation(),
            TimestampColumnPrefix + field->Name,
            tableAlias);
    }

    void ThrowIfExpressionIsOfTypeAny(TStringBuf kind, const TExpression* expression) const
    {
        if (auto* aliasExpr = expression->As<TAliasExpression>()) {
            ThrowIfExpressionIsOfTypeAny(kind, aliasExpr->Expression);
        }

        if (auto* funcExpr = expression->As<TFunctionExpression>()) {
            THROW_ERROR_EXCEPTION_IF(
                NQuery::TryInferFunctionReturnType(funcExpr->FunctionName) == EValueType::Any,
                NOrm::NClient::EErrorCode::InvalidRequestArguments,
                "%v expressions of type \"any\" are not supported",
                kind);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IQueryContext> MakeQueryContext(
    NMaster::IBootstrap* bootstrap,
    TObjectTypeValue objectType,
    ISession* session,
    const TScalarAttributeIndexDescriptor* indexedDescriptor,
    bool allowAnnotations)
{
    return std::make_unique<TQueryContext>(
        bootstrap,
        objectType,
        session,
        indexedDescriptor,
        allowAnnotations);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

std::optional<TYTTransactionDescriptor> GetYTTransactionFromTransactionOrClient(
    const TYTTransactionOrClientDescriptor& ytTransactionOrClient)
{
    if (auto* transaction = std::get_if<TYTTransactionDescriptor>(&ytTransactionOrClient)) {
        return *transaction;
    }
    return std::nullopt;
}

NYT::NApi::IClientPtr GetYTClientFromTransactionOrClient(
    const TYTTransactionOrClientDescriptor& ytTransactionOrClient)
{
    if (auto transactionDescriptor = GetYTTransactionFromTransactionOrClient(ytTransactionOrClient)) {
        return transactionDescriptor->Transaction->GetClient();
    }
    auto* clientDescriptor = std::get_if<TYTClientDescriptor>(&ytTransactionOrClient);
    YT_VERIFY(clientDescriptor);
    return clientDescriptor->Client;
}

struct TPrefetchPassResult
{
    std::vector<TObject*> Objects;
    std::vector<std::pair<TObject*, TCreationAttributeMatches>> CreatedObjects;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

void SubscribeCommitted(
    const TYTTransactionDescriptor& descriptor,
    const ITransaction::TCommittedHandler& onCommitted)
{
    switch (descriptor.Ownership) {
        case EYTTransactionOwnership::Owned:
        case EYTTransactionOwnership::NonOwnedCollocated: {
            descriptor.Transaction->SubscribeCommitted(onCommitted);
            break;
        }
        case EYTTransactionOwnership::NonOwnedAttached: {
            auto* rpcProxyTransaction = descriptor.Transaction->TryAs<NYT::NApi::NRpcProxy::TTransaction>();
            if (!rpcProxyTransaction) {
                THROW_ERROR_EXCEPTION(
                    "Error subscribing to non-owned attached transaction commit: "
                    "only RPC proxy transaction supported for now");
            }

            static_assert(std::is_same_v<
                ITransaction::TCommittedHandlerSignature,
                NYT::NApi::NRpcProxy::TTransaction::TModificationsFlushedHandlerSignature>);
            rpcProxyTransaction->SubscribeModificationsFlushed(onCommitted);
            break;
        }
        default:
            YT_ABORT();
    }
}

void SubscribeAborted(
    const TYTTransactionDescriptor& descriptor,
    const ITransaction::TAbortedHandler& onAborted)
{
    descriptor.Transaction->SubscribeAborted(onAborted);
}

////////////////////////////////////////////////////////////////////////////////

TLogger MakeTransactionLogger(TTransactionId transactionId)
{
    return NObjects::Logger().WithTag("TransactionId: %v", transactionId);
}

////////////////////////////////////////////////////////////////////////////////

void ValidateUpdateIfExistingIds(
    const TObjectManagerPtr& objectManager,
    const std::vector<TCreateObjectSubrequest>& subrequests,
    const std::vector<TKeyAttributeMatches>& matchedAttributesVector)
{
    YT_VERIFY(subrequests.size() == matchedAttributesVector.size());

    for (int index = 0; index < std::ssize(subrequests); ++index) {
        const auto& request = subrequests[index];
        if (request.UpdateIfExisting) {
            auto* typeHandler = objectManager->GetTypeHandlerOrThrow(request.Type);
            const auto& matchedAttributes = matchedAttributesVector[index];
            bool fullKeyPresent = matchedAttributes.Key &&
                (matchedAttributes.ParentKey || !typeHandler->HasParent());
            THROW_ERROR_EXCEPTION_UNLESS(fullKeyPresent,
                "Missing object id or parent id for create with update if existing request");
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TTransaction::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TTransaction* owner,
        NMaster::IBootstrap* bootstrap,
        TTransactionConfigsSnapshot configsSnapshot,
        TTransactionId id,
        TTimestamp startTimestamp,
        TYTTransactionOrClientDescriptor ytTransactionOrClient,
        std::string identityUserTag,
        TTransactionOptions options)
        : Owner_(owner)
        , Bootstrap_(bootstrap)
        , Config_(configsSnapshot.TransactionManager)
        , Id_(id)
        , StartTimestamp_(startTimestamp)
        , Client_(GetYTClientFromTransactionOrClient(ytTransactionOrClient))
        , UnderlyingTransactionDescriptor_(GetYTTransactionFromTransactionOrClient(ytTransactionOrClient))
        , Logger(MakeTransactionLogger(Id_))
        , AccessControlPreloadEnabled_(IsReadWrite())
        , Session_(CreateSession(Bootstrap_, Owner_, configsSnapshot, Logger))
        , StartTime_(TInstant::Now())
        , CurrentRequestStartTime_(StartTime_)
        , ReadPhaseLimit_(Config_->ReadPhaseHardLimit)
        , ExecutionPoolTag_(SelectExecutionPoolTag(
            Bootstrap_->GetAccessControlManager(),
            std::move(identityUserTag),
            Config_->PassUserTagAsSelectExecutionPool))
    {
        if (auto mutatingOptions = std::get_if<TMutatingTransactionOptions>(&options)) {
            MutatingOptions_ = *mutatingOptions;
        } else if (auto readingOptions = std::get_if<TReadingTransactionOptions>(&options)) {
            ReadingOptions_ = *readingOptions;
        } else {
            YT_ABORT();
        }
        if (Config_->AllowRemovalWithNonemptyReferencesOverride &&
            !MutatingOptions_.AllowRemovalWithNonemptyReferences)
        {
            MutatingOptions_.AllowRemovalWithNonemptyReferences = Config_->AllowRemovalWithNonemptyReferencesOverride;
        }
        SingleVersionRetentionConfig_->MinDataTtl = TDuration::Zero();
        SingleVersionRetentionConfig_->MinDataVersions = 1;
        SingleVersionRetentionConfig_->MaxDataVersions = 1;
        // XXX(babenko): YP-777
        SingleVersionRetentionConfig_->IgnoreMajorTimestamp = true;
    }

    ETransactionState GetState() const
    {
        return State_.load();
    }

    TTransactionId GetId() const
    {
        return Id_;
    }

    ISession* GetSession()
    {
        return Session_.get();
    }

    NMaster::IBootstrap* GetBootstrap()
    {
        return Bootstrap_;
    }

    TTransactionManagerConfigPtr GetConfig() const
    {
        return Config_;
    }

    const std::optional<TString>& GetExecutionPoolTag()
    {
        return ExecutionPoolTag_;
    }

    void EnsureReadWrite() const
    {
        YT_VERIFY(IsReadWrite());
    }

    void EnsureNotPrecommitted() const
    {
        auto currentState = State_.load();
        if (currentState >= ETransactionState::Precommitted) {
            THROW_ERROR_EXCEPTION("Using transaction %v after running precommit actions is forbidden",
                Id_)
                << TErrorAttribute("current_state", currentState);
        }
    }

    const TMutatingTransactionOptions& GetMutatingTransactionOptions() const
    {
        return MutatingOptions_;
    }

    std::unique_ptr<IUpdateContext> CreateUpdateContext()
    {
        EnsureReadWrite();

        return std::make_unique<TUpdateContext>(this);
    }

    std::unique_ptr<ILoadContext> CreateLoadContext(
        TTestingStorageOptions testingStorageOptions)
    {
        return std::make_unique<TLoadContext>(this, std::move(testingStorageOptions));
    }

    std::unique_ptr<IStoreContext> CreateStoreContext()
    {
        EnsureReadWrite();

        return std::make_unique<TStoreContext>(this);
    }

    std::vector<TObject*> CreateObjects(
        std::vector<TCreateObjectSubrequest> subrequests,
        const TTransactionCallContext& transactionCallContext)
    {
        EnsureNotPrecommitted();
        EnsureReadWrite();

        auto context = CreateUpdateContext();

        auto objects = AbortOnException([&] {
            return DoCreateObjects(std::move(subrequests), context.get(), transactionCallContext);
        });

        context->Commit();
        return objects;
    }

    TObject* CreateObjectInternal(
        TObjectTypeValue type,
        TObjectKey key,
        TObjectKey parentKey,
        bool allowExisting)
    {
        EnsureNotPrecommitted();
        EnsureReadWrite();

        auto* object = AbortOnException([&] {
            return Session_->CreateObject(
                type,
                std::move(key),
                std::move(parentKey),
                allowExisting);
        });
        if (object->GetState() == EObjectState::Creating) {
            object->GetTypeHandler()->FinishObjectCreation(Owner_, object);
        }
        return object;
    }

    TAccessControlPermissionValue GetPermissionForPathUpdate(const NYPath::TYPath& path)
    {
        if (Config_->UseAdministerPermission && HasPrefix(path, AclAttributePath)) {
            return TAccessControlPermissionValues::Administer;
        } else {
            return TAccessControlPermissionValues::Write;
        }
    }

    void UpdateObjects(
        std::vector<TObjectUpdateRequest> objectUpdateRequests,
        IUpdateContext* context,
        const TTransactionCallContext& transactionCallContext)
    {
        EnsureNotPrecommitted();
        EnsureReadWrite();

        std::unique_ptr<IUpdateContext> updateContext;
        if (!context) {
            updateContext = CreateUpdateContext();
            context = updateContext.get();
        }

        ValidateAccessControlPermissions(Bootstrap_->GetAccessControlManager(), objectUpdateRequests);

        {
            TTraceContextGuard guard(CreateTraceContextFromCurrent("NYT::NOrm::TTransaction::DoUpdateObjects"));

            for (const auto& request : objectUpdateRequests) {
                AbortOnException([&] {
                    DoUpdateObject(
                        request.Object,
                        request.Updates,
                        request.Prerequisites,
                        context,
                        transactionCallContext,
                        request.Consumer);
                });
            }
        }

        if (updateContext) {
            updateContext->Commit();
        }
    }

    std::vector<NAccessControl::TObjectPermission> GetRemoveObjectsPermissionsToValidate(
        const std::vector<TObject*>& objects)
    {
        TTraceContextGuard guard(CreateTraceContextFromCurrent(
            "NYT::NOrm::TTransaction::GetRemoveObjectsPermissionsToValidate"));

        std::vector<NAccessControl::TObjectPermission> objectPermissions;
        objectPermissions.reserve(objects.size());
        for (auto* object : objects) {
            object->ValidateExists();
            objectPermissions.push_back(NAccessControl::TObjectPermission{
                .Object = object,
                .AttributePath = TYPath(RemoveObjectAccessControlPath),
                .Permission = TAccessControlPermissionValues::Write,
            });
        }
        return objectPermissions;
    }

    void RemoveObjects(std::vector<TObject*> objects)
    {
        EnsureNotPrecommitted();
        EnsureReadWrite();

        if (objects.empty()) {
            return;
        }

        const auto& accessControlManager = Bootstrap_->GetAccessControlManager();

        auto objectPermissions = GetRemoveObjectsPermissionsToValidate(objects);

        AbortOnException([&] {
            accessControlManager->ValidatePermissions(std::move(objectPermissions));
            Session_->RemoveObjects(std::move(objects));
        });
    }

    void RemoveObjectInternal(TObject* object)
    {
        EnsureNotPrecommitted();
        EnsureReadWrite();

        AbortOnException([&] {
            object->ValidateExists();
            Session_->RemoveObject(object);
        });
    }

    IUnversionedRowsetPtr SelectFields(
        TObjectTypeValue type,
        const std::vector<const TDBField*>& fields,
        std::source_location location)
    {
        TQueryContext queryContext(Bootstrap_, type, GetSession());

        for (const auto* field : fields) {
            auto fieldExpression = queryContext.GetFieldExpression(field);
            queryContext.GetQuery()->SelectExprs->push_back(std::move(fieldExpression));
        }

        EnsureNonEmptySelectExpressions(&queryContext, queryContext.GetQuery());

        queryContext.GetQuery()->WherePredicate = TExpressionList{
            BuildObjectFilterByNullColumn(&queryContext, &ObjectsTable.Fields.MetaRemovalTime)
        };

        auto queryString = queryContext.Finalize();

        YT_LOG_DEBUG("Selecting objects (Type: %v, Query: %v)",
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(type),
            queryString);

        return RunSelect(std::move(queryString), location);
    }

    TObject* GetObject(TObjectTypeValue type, TObjectKey key, TObjectKey parentKey = {})
    {
        return Session_->GetObject(type, std::move(key), std::move(parentKey));
    }

    void DoRunPrecommitActions(TTransactionContext context)
    {
        TTraceContextGuard guard(CreateTraceContextFromCurrent("NYT::NOrm::TTransaction::RunPrecommitActions"));

        EnsureNotPrecommitted();

        try {
            YT_LOG_DEBUG("Validating updates");
            for (const auto& [_, validator] : Validators_) {
                validator();
            }

            YT_LOG_DEBUG("Preparing for commit");
            Owner_->PrepareCommit();

            if (Config_->CommitDelay) {
                TDelayedExecutor::WaitForDuration(*Config_->CommitDelay);
            }

            YT_LOG_DEBUG("Flushing transaction");
            MutatingOptions_.TransactionContext.MergeContext(std::move(context));
            Session_->FlushTransaction(MutatingOptions_);
        } catch (const std::exception& ex) {
            State_.store(ETransactionState::Failed);

            THROW_ERROR_EXCEPTION("Error preparing transaction commit")
                << ex;
        }

        State_.store(ETransactionState::Precommitted);
    }

    void RunPrecommitActions(TTransactionContext context)
    {
        const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
        auto identity = TryGetAuthenticatedUserIdentity().value_or(NRpc::GetRootAuthenticationIdentity());
        const TAuthenticatedUserGuard authUserGuard{
            accessControlManager,
            identity,
            TryGetAuthenticatedUserTicket()
        };

        DoRunPrecommitActions(context);
    }

    TTransactionCommitResult DoCommit(
        TTransactionContext context,
        NRpc::TAuthenticationIdentity identity,
        const std::string& userTicket)
    {
        TTraceContextGuard guard(CreateTraceContextFromCurrent("NYT::NOrm::TTransaction::DoCommit"));

        TAuthenticatedUserGuard authUserGuard{
            Bootstrap_->GetAccessControlManager(),
            identity,
            userTicket
        };

        if (!IsPrecommitted()) {
            DoRunPrecommitActions(context);
        }

        switch (UnderlyingTransactionDescriptor_->Ownership) {
            case EYTTransactionOwnership::Owned: {
                auto commitResultOrError = WaitFor(UnderlyingTransactionDescriptor_->Transaction->Commit());

                if (!commitResultOrError.IsOK()) {
                    State_.store(ETransactionState::Failed);

                    THROW_ERROR_EXCEPTION("Error committing owned transaction")
                        << commitResultOrError;
                }

                const auto& commitResult = commitResultOrError.Value();
                auto timestamp = commitResult.PrimaryCommitTimestamp;
                if (timestamp == NHiveClient::NullTimestamp) {
                    if (commitResult.CommitTimestamps.Timestamps.empty()) {
                        // Transaction contains no modifications, any timestamp >= start timestamp is OK.
                        timestamp = GetStartTimestamp();
                    } else {
                        // For chaos transaction over async replica `PrimaryCommitTimestamp` is null
                        // because the replica does not participate in the transaction commit.
                        timestamp = commitResult.CommitTimestamps.Timestamps[0].second;
                        for (const auto& commitTimestamp : commitResult.CommitTimestamps.Timestamps) {
                            YT_VERIFY(timestamp == commitTimestamp.second);
                        }
                    }
                }

                State_.store(ETransactionState::Committed);

                YT_LOG_DEBUG("Owned transaction committed (CommitTimestamp: %v)",
                    timestamp);

                Owner_->PostCommit();

                return TTransactionCommitResult{
                    .CommitTimestamp = timestamp,
                    .StartTime = GetCommitStartTime(),
                    .FinishTime = GetCommitFinishTime(),
                };
            }
            case EYTTransactionOwnership::NonOwnedCollocated: {
                State_.store(ETransactionState::Committed);

                YT_LOG_DEBUG("Non-owned collocated transaction committed");

                Owner_->PostCommit();

                return TTransactionCommitResult{
                    .StartTime = GetCommitStartTime(),
                    .FinishTime = GetCommitFinishTime(),
                };
            }
            case EYTTransactionOwnership::NonOwnedAttached: {
                auto* rpcProxyTransaction = UnderlyingTransactionDescriptor_
                    ->Transaction->TryAs<NYT::NApi::NRpcProxy::TTransaction>();
                if (!rpcProxyTransaction) {
                    THROW_ERROR_EXCEPTION(
                        "Error committing non-owned attached transaction: "
                        "underlying transaction flush is supported only for RPC proxy transactions for now");
                }
                // #FlushModifications is used instead of #Flush to flush modifications
                // without the underlying transaction state change on RPC proxy server-side.
                // Transaction state management is a responsibility of the transaction owner.
                auto flushResultOrError = WaitFor(rpcProxyTransaction->FlushModifications());
                if (!flushResultOrError.IsOK()) {
                    State_.store(ETransactionState::Failed);

                    THROW_ERROR_EXCEPTION(
                        "Error committing non-owned attached transaction: "
                        "failed to flush underlying transaction")
                        << flushResultOrError;
                }

                State_.store(ETransactionState::Committed);

                YT_LOG_DEBUG("Non-owned attached transaction committed");

                Owner_->PostCommit();

                return TTransactionCommitResult{
                    .StartTime = GetCommitStartTime(),
                    .FinishTime = GetCommitFinishTime(),
                };
            }
            default:
                YT_ABORT();
        }
    }

    TFuture<TTransactionCommitResult> Commit(TTransactionContext context) noexcept
    {
        TExtendedCallback<TFuture<TTransactionCommitResult>()> callback;
        {
            TForbidContextSwitchGuard contextSwitchGuard;

            EnsureReadWrite();

            auto oldState = State_.exchange(ETransactionState::Committing);
            if (oldState == ETransactionState::Committing ||
                oldState == ETransactionState::Committed)
            {
                return MakeFuture<TTransactionCommitResult>(
                    TError("Tried to commit transaction twice"));
            }

            auto identity = NAccessControl::TryGetAuthenticatedUserIdentity().value_or(
                NRpc::GetCurrentAuthenticationIdentity());

            callback = BIND(
                &TTransaction::TImpl::DoCommit,
                MakeStrong(this),
                Passed(std::move(context)),
                identity,
                TryGetAuthenticatedUserTicket())
                .AsyncVia(GetCurrentInvoker());
        }
        return callback.Run();
    }

    TFuture<void> Abort() noexcept
    {
        TExtendedCallback<TFuture<void>()> callback;
        {
            TForbidContextSwitchGuard contextSwitchGuard;

            if (!UnderlyingTransactionDescriptor_) {
                // This is a read-only transaction.
                YT_VERIFY(AfterAbortActions_.empty());
                return VoidFuture;
            }

            YT_LOG_DEBUG("Transaction aborted");
            auto oldState = State_.exchange(ETransactionState::Aborted);
            if (oldState == ETransactionState::Aborted) {
                YT_LOG_WARNING("Tried to abort transaction twice");
                return VoidFuture;
            }

            if (!AfterAbortActions_.empty()) {
                callback = BIND(
                    &TTransaction::TImpl::RunAfterAbortActions,
                    MakeStrong(this))
                    .AsyncVia(GetCurrentInvoker());
            }
        }

        std::vector<TFuture<void>> futures;
        if (callback) {
            futures.push_back(callback.Run());
        }
        if (UnderlyingTransactionDescriptor_->Ownership == EYTTransactionOwnership::Owned) {
            futures.push_back(UnderlyingTransactionDescriptor_->Transaction->Abort());
        }

        return AllSet(std::move(futures)).AsVoid();
    }

    bool ScheduleCommitAction(TCommitActionType type, TObject* object)
    {
        EnsureReadWrite();
        return CommitActions_[type].insert(object).second;
    }

    THashSet<TObject*> ExtractCommitActions(TCommitActionType type)
    {
        THashSet<TObject*> result;

        if (auto it = CommitActions_.find(type); it != CommitActions_.end()) {
            YT_LOG_DEBUG("Extracting commit actions (Type: %v, Count: %v)",
                type,
                it->second.size());
            result = std::move(it->second);
            CommitActions_.erase(it);
        }
        return result;
    }

    TAsyncSemaphoreGuard AcquireLock()
    {
        EnsureReadWrite();

        return WaitForUnique(Semaphore_->AsyncAcquire(/*slots*/ 1))
            .ValueOrThrow();
    }

    TPerformanceStatistics FlushPerformanceStatistics()
    {
        TPerformanceStatistics result;
        std::swap(result, CurrentPerformanceStatistics_);
        return result;
    }

    const TPerformanceStatistics& GetTotalPerformanceStatistics()
    {
        return TotalPerformanceStatistics_;
    }

    void EnableAccessControlPreload()
    {
        AccessControlPreloadEnabled_ = true;
    }

    bool AccessControlPreloadEnabled() const
    {
        return AccessControlPreloadEnabled_;
    }

    void UpdateRequestTimeout(std::optional<TDuration> timeout)
    {
        CurrentRequestStartTime_ = TInstant::Now();
        RequestTimeout_ = std::move(timeout);
        UpdateRequestTimeoutRemaining();
    }

    void UpdateRequestTimeoutRemaining()
    {
        if (RequestTimeout_) {
            RequestTimeoutRemaining_ = Config_->EnableDeadlinePropagation
                ? Max(
                    Config_->MinYTRequestTimeout,
                    *RequestTimeout_ - (TInstant::Now() - CurrentRequestStartTime_))
                : *RequestTimeout_;
        } else {
            RequestTimeoutRemaining_.reset();
        }
    }

    std::optional<TDuration> GetRequestTimeoutRemaining()
    {
        if (RequestTimeout_) {
            return *RequestTimeout_ - (TInstant::Now() - CurrentRequestStartTime_);
        }
        return std::nullopt;
    }

    void CheckChildrenFinalizers()
    {
        auto objects = ExtractCommitActions(TCommitActionTypes::RemoveChildrenFinalizers);
        if (objects.empty()) {
            return;
        }

        YT_LOG_DEBUG("Checking children finalizers");
        for (auto* object : objects) {
            YT_VERIFY(object->Finalizers().Load().contains(ChildrenFinalizer));
            object->GetTypeHandler()->ForEachChildrenAttribute(
                object,
                [] (const TChildrenAttributeBase* childrenAttribute) {
                    childrenAttribute->ScheduleLoad();
                });
        }
        for (auto* object : objects) {
            object->GetTypeHandler()->ForEachChildrenAttribute(
                object,
                [] (const TChildrenAttributeBase* childrenAttribute) {
                    for (const auto* child : childrenAttribute->UntypedLoad()) {
                        child->Finalizers().ScheduleLoad();
                    }
                });
        }
        for (auto* object : objects) {
            bool hasOtherFinalizingChildren = false;
            object->GetTypeHandler()->ForEachChildrenAttribute(
                object,
                [&hasOtherFinalizingChildren] (const TChildrenAttributeBase* childrenAttribute)
                {
                    for (const auto* child : childrenAttribute->UntypedLoad()) {
                        auto activeFinalizersCount = child->CountActiveFinalizers();
                        if (activeFinalizersCount > 1 ||
                            (activeFinalizersCount == 1 && !child->IsFinalizerActive(ParentFinalizer)))
                        {
                            hasOtherFinalizingChildren = true;
                            break;
                        }
                    }
                });
            if (!hasOtherFinalizingChildren) {
                // NB: Removal of finalizer may schedule parent removal
                // to `RemoveFinalizedObjects` commit action. Other children removals would cascade afterwards.
                object->CompleteFinalizer(ChildrenFinalizer);
            } else {
                Session_->ScheduleStore([object] (IStoreContext* context) {
                    context->WriteRow(
                        object->GetTypeHandler()->GetTable(),
                        object->GetTypeHandler()->GetObjectTableKey(object),
                        std::array{
                            &ObjectsTable.Fields.ExistenceLock,
                        },
                        ToUnversionedValues(
                            context->GetRowBuffer(),
                            false));
                });
            }
        }
    }

    void PrepareCommit()
    {
        CommitStartTime_ = TInstant::Now();

        auto historyManger = Bootstrap_->MakeHistoryManager(Owner_);
        if (CommitStartTimestampRequired(historyManger.Get())) {
            CommitStartTimestamp_ = WaitFor(Bootstrap_->GetTransactionManager()->GenerateTimestamps())
                .ValueOrThrow();
        }

        PerformAttributeMigrations();

        for (auto* object : ExtractCommitActions(TCommitActionTypes::HandleRevisionUpdates)) {
            if (object->RemovalStarted()) {
                continue;
            }
            object->ReconcileRevisionTrackers();
        }
        CheckChildrenFinalizers();

        while (auto finalizedObjectsSet = ExtractCommitActions(TCommitActionTypes::RemoveFinalizedObjects)) {
            YT_LOG_DEBUG("Removing finalized objects");
            std::vector<TObject*> toRemove;
            std::ranges::copy_if(finalizedObjectsSet, std::back_inserter(toRemove), [] (TObject* object) {
                return object->IsFinalized() && !object->HasActiveFinalizers();
            });
            std::ranges::sort(
                toRemove,
                std::ranges::greater(),
                [] (TObject* object) {
                    return object->GetTypeHandler()->GetAncestryDepth();
                });
            RemoveObjects(std::move(toRemove));
            CheckChildrenFinalizers();
        }
    }

    void PostCommit()
    {
        RunAfterCommitActions();
        CommitFinishTime_ = TInstant::Now();
        Bootstrap_->GetTransactionManager()
            ->ProfilePerformanceStatistics(TotalPerformanceStatistics_);
        Session_->ProfileObjectsAttributes();
    }

    TTimestamp GetStartTimestamp() const
    {
        return StartTimestamp_;
    }

    TTimestamp GetStartCommitTimestamp() const
    {
        YT_LOG_FATAL_IF(CommitStartTimestamp_ == 0,
            "Cannot provide commit start timestamp: commit phase has not started yet");
        return CommitStartTimestamp_;
    }

    TInstant GetStartTime() const
    {
        return StartTime_;
    }

    TInstant GetCommitStartTime() const
    {
        YT_LOG_FATAL_IF(CommitStartTime_ == TInstant::Zero(),
            "Transaction commit start time is undefined: transaction commit has not started yet");
        return CommitStartTime_;
    }

    TInstant GetCommitFinishTime() const
    {
        YT_LOG_FATAL_IF(CommitFinishTime_ == TInstant::Zero(),
            "Transaction commit finish time is undefined: transaction was not committed yet");
        return CommitFinishTime_;
    }

    THistoryTime GetHistoryEventTime(const THistoryTableBase* history) const
    {
        auto historyTimeMode = history->TimeMode;
        auto historyCommitTime = history->CommitTime;

        switch (historyTimeMode) {
            case EHistoryTimeMode::Logical:
                switch (historyCommitTime) {
                    case EHistoryCommitTime::TransactionStart:
                        return GetStartTimestamp();
                    case EHistoryCommitTime::TransactionCommitStart:
                        return GetStartCommitTimestamp();
                }
            case EHistoryTimeMode::Physical:
                switch (historyCommitTime) {
                    case EHistoryCommitTime::TransactionStart:
                        return GetStartTime();
                    case EHistoryCommitTime::TransactionCommitStart:
                        return GetCommitStartTime();
                }
        }
    }

    void SetReadPhaseLimit(i64 limit)
    {
        ReadPhaseLimit_ = limit;
    }

    void AllowFullScan(bool allowFullScan)
    {
        ReadingOptions_.AllowFullScan.emplace(allowFullScan);
    }

    bool FullScanAllowed() const
    {
        return ReadingOptions_.AllowFullScan.value_or(Config_->FullScanAllowedByDefault);
    }

    void AllowRemovalWithNonEmptyReferences(bool allow)
    {
        THROW_ERROR_EXCEPTION_IF(MutatingOptions_.AllowRemovalWithNonemptyReferences.value_or(allow) != allow,
            "Cannot redefine AllowRemovalWithNonEmptyReference option inside transaction: was %v, got %v",
            MutatingOptions_.AllowRemovalWithNonemptyReferences,
            allow);
        MutatingOptions_.AllowRemovalWithNonemptyReferences.emplace(allow);
    }

    std::optional<bool> RemovalWithNonEmptyReferencesAllowed() const
    {
        return MutatingOptions_.AllowRemovalWithNonemptyReferences;
    }

    void AddAfterCommitAction(TAction action)
    {
        EnsureReadWrite();
        AfterCommitActions_.push_back(std::move(action));
    }

    void AddAfterAbortAction(TAction action)
    {
        EnsureReadWrite();
        AfterAbortActions_.push_back(std::move(action));
    }

    void PerformAttributeMigrations()
    {
        if (!GetConfig()->EnableAttributeMigrations) {
            return;
        }

        auto objects = ExtractCommitActions(TCommitActionTypes::HandleAttributeMigrations);

        {
            TTraceContextGuard guard(
                CreateTraceContextFromCurrent("NYT::NOrm::TTransaction::PrepareAttributeMigrations"));
            for (auto* object : objects) {
                object->PrepareAttributeMigrations();
            }
        }

        {
            TTraceContextGuard guard(
                CreateTraceContextFromCurrent("NYT::NOrm::TTransaction::FinalizeAttributeMigrations"));
            for (auto* object : objects) {
                object->FinalizeAttributeMigrations();
            }
        }
    }

private:
    class TUpdateContext
        : public IUpdateContext
    {
    public:
        explicit TUpdateContext(TTransaction::TImplPtr transaction)
            : Transaction_(std::move(transaction))
        { }

        void AddPreparer(std::function<void(IUpdateContext*)> preparer) override
        {
            Preparers_.push_back(std::move(preparer));
        }

        void AddFinalizer(std::function<void(IUpdateContext*)> finalizer) override
        {
            Finalizers_.push_back(std::move(finalizer));
        }

        void Commit() override
        {
            TTraceContextGuard guard(CreateTraceContextFromCurrent("NYT::NOrm::TTransaction::TUpdateContext::Commit"));

            const auto& Logger = Transaction_->Logger;
            Transaction_->AbortOnException([&] {
                YT_LOG_DEBUG("Flushing update context");
                while (!Preparers_.empty() || !Finalizers_.empty()) {
                    decltype(Preparers_) preparers;
                    std::swap(Preparers_, preparers);
                    decltype(Finalizers_) finalizers;
                    std::swap(Finalizers_, finalizers);

                    {
                        TTraceContextGuard guard(
                            CreateTraceContextFromCurrent("NYT::NOrm::TTransaction::TUpdateContext::RunPreparers"));

                        YT_LOG_DEBUG("Preparing updates");
                        for (const auto& preparer : preparers) {
                            preparer(this);
                        }
                    }
                    {
                        TTraceContextGuard guard(
                            CreateTraceContextFromCurrent("NYT::NOrm::TTransaction::TUpdateContext::RunFinalizers"));

                        YT_LOG_DEBUG("Finalizing updates");
                        for (const auto& finalizer : finalizers) {
                            finalizer(this);
                        }
                    }
                }
            });
        }

    private:
        const TTransaction::TImplPtr Transaction_;

        std::vector<std::function<void(IUpdateContext*)>> Preparers_;
        std::vector<std::function<void(IUpdateContext*)>> Finalizers_;
    }; // class TUpdateContext

    class TPersistenceContextBase
    {
    protected:
        TTransaction::TImpl* const Transaction_;

        struct TRowBufferTag { };
        const TRowBufferPtr RowBuffer_ = New<TRowBuffer>(TRowBufferTag());

        explicit TPersistenceContextBase(TTransaction::TImpl* transaction)
            : Transaction_(transaction)
        { }

        TUnversionedRow CaptureKey(const TObjectKey& key)
        {
            return ToUnversionedRow(key, RowBuffer_);
        }

        std::vector<TUnversionedRow> CaptureKeys(const TRange<TObjectKey>& keys)
        {
            std::vector<TUnversionedRow> capturedKeys;
            capturedKeys.reserve(keys.size());
            for (const auto& key : keys) {
                capturedKeys.push_back(CaptureKey(key));
            }
            return capturedKeys;
        }
    }; // class TPersistenceContextBase

    class TLoadContext
        : public TPersistenceContextBase
        , public ILoadContext
    {
    public:
        TLoadContext(TTransaction::TImpl* transaction, TTestingStorageOptions testingStorageOptions)
            : TPersistenceContextBase(transaction)
            , Lookups_(this, testingStorageOptions.FailLookups)
            , Selects_(this, testingStorageOptions.FailSelects)
            , PullQueues_(this)
        { }

        const TRowBufferPtr& GetRowBuffer() override
        {
            return RowBuffer_;
        }

        TString GetTablePath(const TDBTable* table) override
        {
            const auto& ytConnector = Transaction_->Bootstrap_->GetYTConnector();
            return ytConnector->GetTablePath(table);
        }

        void ScheduleLookup(
            const TDBTable* table,
            const TObjectKey& key,
            TRange<const TDBField*> fields,
            std::function<void(const std::optional<TRange<TUnversionedValue>>&)> handler) override
        {
            Lookups_.AddRequest<TUnversionedValue>(table, key, fields, std::move(handler));
        }

        void ScheduleLookup(
            const TDBTable* table,
            TRange<TObjectKey> keys,
            TRange<const TDBField*> fields,
            std::function<void(TSharedRange<TUnversionedRow>, const TDynBitMap&)> handler) override
        {
            Lookups_.AddRequest<TUnversionedValue>(table, keys, fields, std::move(handler));
        }

        void ScheduleVersionedLookup(
            const TDBTable* table,
            const TObjectKey& key,
            TRange<const TDBField*> fields,
            std::function<void(const std::optional<TRange<TVersionedValue>>&)> handler) override
        {
            Lookups_.AddRequest<TVersionedValue>(table, key, fields, std::move(handler));
        }

        void ScheduleVersionedLookup(
            const TDBTable* table,
            TRange<TObjectKey> keys,
            TRange<const TDBField*> fields,
            std::function<void(TSharedRange<TVersionedRow>, const TDynBitMap&)> handler) override
        {
            Lookups_.AddRequest<TVersionedValue>(table, keys, fields, std::move(handler));
        }


        void ScheduleSelect(
            std::string query,
            std::function<void(const IUnversionedRowsetPtr&)> handler,
            NTableClient::EVersionedIOMode versionedIOMode) override
        {
            Selects_.AddRequest(std::move(query), versionedIOMode, std::move(handler));
        }

        void SchedulePullQueue(
            const TDBTable* table,
            std::optional<i64> offset,
            int partitionIndex,
            std::optional<i64> rowCountLimit,
            const std::optional<NYPath::TYPath>& consumerPath,
            std::function<void(const NQueueClient::IQueueRowsetPtr&)> handler) override
        {
            THROW_ERROR_EXCEPTION_UNLESS(offset || consumerPath,
                "PullQueue cannot be used w/o offset if no consumer is specified");
            PullQueues_.AddRequest(table, offset, partitionIndex, rowCountLimit, consumerPath, std::move(handler));
        }

        void RunReads() override
        {
            const auto& Logger = Transaction_->Logger;

            if (Selects_.Empty() && PullQueues_.Empty() && Lookups_.Empty()) {
                YT_LOG_DEBUG("Call \"RunReads\", but list of requests is empty");
                return;
            }

            ++Transaction_->CurrentPerformanceStatistics_.ReadPhaseCount;
            ++Transaction_->TotalPerformanceStatistics_.ReadPhaseCount;
            THROW_ERROR_EXCEPTION_IF(Transaction_->ReadPhaseLimit_ &&
                Transaction_->TotalPerformanceStatistics_.ReadPhaseCount >= Transaction_->ReadPhaseLimit_,
                "Too many read phases, expected less than %v",
                Transaction_->ReadPhaseLimit_);

            {
                std::vector<TFuture<void>> asyncResults;
                asyncResults.reserve(Selects_.Size() + PullQueues_.Size() + Lookups_.Size());

                Transaction_->UpdateRequestTimeoutRemaining();

                std::ranges::move(Selects_.ExecuteRequests(), std::back_inserter(asyncResults));
                std::ranges::move(PullQueues_.ExecuteRequests(), std::back_inserter(asyncResults));
                std::ranges::move(Lookups_.ExecuteRequests(), std::back_inserter(asyncResults));

                WaitFor(AllSucceeded(std::move(asyncResults)))
                    .ThrowOnError();
            }
            {
                Selects_.ParseResult();
                PullQueues_.ParseResult();
                Lookups_.ParseResult();
            }
            {
                std::vector<TError> errors;

                std::ranges::move(Selects_.HandleResult(), std::back_inserter(errors));
                std::ranges::move(PullQueues_.HandleResult(), std::back_inserter(errors));
                std::ranges::move(Lookups_.HandleResult(), std::back_inserter(errors));

                if (!errors.empty()) {
                    THROW_ERROR_EXCEPTION("Error parsing and processing fetched data")
                        << std::move(errors);
                }
            }
        }

    private:
        class TRequestsProcessor
        {
        public:
            TRequestsProcessor(TLoadContext* owner)
                : Owner_(owner)
                , Logger(Owner_->Transaction_->Logger)
            { }

            virtual size_t Size() const = 0;

            bool Empty() const
            {
                return Size() == 0;
            }

            std::vector<TFuture<void>> ExecuteRequests()
            {
                YT_VERIFY(State_ == ETransactionLoadRequestsState::Initialized);
                auto futures = DoExecuteRequests();
                State_ = ETransactionLoadRequestsState::Requested;
                return futures;
            }

            void ParseResult()
            {
                YT_VERIFY(State_ == ETransactionLoadRequestsState::Requested);
                DoParseResult();
                State_ = ETransactionLoadRequestsState::ParsedResults;
            }

            std::vector<TError> HandleResult() const
            {
                YT_VERIFY(State_ == ETransactionLoadRequestsState::ParsedResults);

                return DoHandleResult();
            }

        protected:
            TLoadContext* const Owner_;
            const TLogger& Logger;

        private:
            enum class ETransactionLoadRequestsState
            {
                Initialized,
                Requested,
                ParsedResults,
            };

            ETransactionLoadRequestsState State_ = ETransactionLoadRequestsState::Initialized;

            virtual std::vector<TFuture<void>> DoExecuteRequests() = 0;
            virtual void DoParseResult() = 0;
            virtual std::vector<TError> DoHandleResult() const = 0;
        }; // class TRequestsProcessor

        class TLookupRequestsProcessor final
            : public TRequestsProcessor
        {
        public:
            TLookupRequestsProcessor(TLoadContext* owner, bool failLookups)
                : TRequestsProcessor(owner)
                , FailLookups_(failLookups)
            { }

            template <COneOf<TUnversionedValue, TVersionedValue> TValue,
                COneOf<TObjectKey, TRange<TObjectKey>> TKeys, class THandler>
            void AddRequest(
                const TDBTable* table,
                const TKeys& keys,
                TRange<const TDBField*> fields,
                THandler handler)
            {
                // YT-15516: Workaround inconsistency between RPC proxy and native clients.
                if (fields.empty()) {
                    THROW_ERROR_EXCEPTION("Empty list of fields by key(s) %v from table %v cannot be looked up",
                        std::span(keys),
                        table->GetName());
                }

                auto& tableToContext = [this] () -> auto& {
                    if constexpr (std::same_as<TValue, TUnversionedValue>) {
                        return TableToUnversionedLookupContext_;
                    } else {
                        return TableToVersionedLookupContext_;
                    }
                }();

                auto it = tableToContext.find(table);
                if (it == tableToContext.end()) {
                    std::tie(it, std::ignore) = tableToContext.try_emplace(
                        table, Owner_->GetTablePath(table), TExtendedNameTable(table));
                }

                if constexpr (std::same_as<TKeys, TObjectKey>) {
                    it->second.Requests.emplace_back(
                        Owner_->CaptureKey(keys),
                        TOptimalVector<const TDBField*>(fields.begin(), fields.end()),
                        std::move(handler));
                } else {
                    it->second.MultikeyRequests.emplace_back(
                        Owner_->CaptureKeys(keys),
                        TOptimalVector<const TDBField*>(fields.begin(), fields.end()),
                        std::move(handler));
                }
            }

            size_t Size() const override
            {
                return TableToVersionedLookupContext_.size() + TableToUnversionedLookupContext_.size();
            }

        private:
            // Store typical field count per lookup request (4) inline.
            template<class T>
            using TOptimalVector = TCompactVector<T, 4>;

            const bool FailLookups_;

            template <bool Versioned>
            struct TLookupContext
            {
                using THandledValue = std::conditional_t<Versioned, TVersionedValue, TUnversionedValue>;
                using TDBRequestResult = std::conditional_t<
                    Versioned, TVersionedLookupRowsResult, TUnversionedLookupRowsResult>;
                using TRow = std::conditional_t<Versioned, TVersionedRow, TUnversionedRow>;

                struct TRequest
                {
                    TUnversionedRow Key;
                    TOptimalVector<const TDBField*> Fields;
                    std::function<void(const std::optional<TRange<THandledValue>>&)> Handler;
                };

                struct TMultikeyRequest
                {
                    std::vector<TUnversionedRow> Keys;
                    TOptimalVector<const TDBField*> Fields;
                    std::function<void(TSharedRange<TRow>, const TDynBitMap&)> Handler;
                };

                struct TOptimizedRequest
                {
                    std::vector<TUnversionedRow> Keys;
                    std::vector<int> Fields;
                    TFuture<TDBRequestResult> AsyncResult;

                    int GetFieldIdChecked(int valueId) const
                    {
                        YT_VERIFY(0 <= valueId && valueId < std::ssize(Fields));
                        return Fields[valueId];
                    }
                };

                using TRowResult = TOptimalVector<std::optional<THandledValue>>;

                TString TablePath;
                TExtendedNameTable NameTable;
                std::vector<TRequest> Requests;
                std::vector<TMultikeyRequest> MultikeyRequests;
                std::vector<TOptimizedRequest> OptimizedRequests;
                // The following matrix is indexed by (key, fieldId).
                // NB! It stores non-owning versioned values: holders are stored in the optimized requests async results.
                THashMap<TUnversionedRow, TRowResult> Results;

                TString FormatOptimizedRequestForKey(
                    const TOptimizedRequest& request,
                    TUnversionedRow key) const
                {
                    auto formattedColumns = MakeFormattableView(
                        request.Fields,
                        [this] (auto* builder, int fieldId) {
                            FormatValue(
                                builder,
                                NameTable.GetFieldById(fieldId)->Name,
                                TStringBuf());
                        });
                    return Format("Path: %v, Columns: %v, Key: %v", TablePath, formattedColumns, key);
                }

                TString FormatOptimizedRequest(
                    const TOptimizedRequest& request,
                    i64 keysLimit,
                    i64 readPhaseIndex) const
                {
                    auto formattedColumns = MakeFormattableView(
                        request.Fields,
                        [this] (auto* builder, int fieldId) {
                            FormatValue(
                                builder,
                                NameTable.GetFieldById(fieldId)->Name,
                                TStringBuf());
                        });
                    auto formattedKeys = MakeShrunkFormattableView(request.Keys, TDefaultFormatter(), keysLimit);
                    return Format("Path: %v, Columns: %v, KeyCount: %v, Keys: %v, ReadPhaseIndex: %v",
                        TablePath,
                        formattedColumns,
                        request.Keys.size(),
                        formattedKeys,
                        readPhaseIndex);
                }
            };

            using TVersionedLookupContext = TLookupContext</*Versioned*/ true>;
            using TUnversionedLookupContext = TLookupContext</*Versioned*/ false>;

            THashMap<const TDBTable*, TVersionedLookupContext> TableToVersionedLookupContext_;
            THashMap<const TDBTable*, TUnversionedLookupContext> TableToUnversionedLookupContext_;

            template <CWithAnyBoolArg<TLookupContext> TContext>
            void PrepareOptimizedRequests(TContext& context, ssize_t maxKeysPerLookupRequest)
            {
                YT_LOG_DEBUG("Building optimized lookup requests (Path: %v)", context.TablePath);

                THashMap<TUnversionedRow, TDynBitMap> columnsRequestedForKey;
                columnsRequestedForKey.reserve(context.Requests.size());
                for (const auto& request : context.Requests) {
                    auto& columnsBitmap = columnsRequestedForKey[request.Key];
                    for (const auto* field : request.Fields) {
                        columnsBitmap.Set(context.NameTable.GetIdByFieldOrRegister(field));
                    }
                }

                for (const auto& request : context.MultikeyRequests) {
                    TDynBitMap commonColumnsBitmap;
                    for (const auto* field : request.Fields) {
                        commonColumnsBitmap.Set(context.NameTable.GetIdByFieldOrRegister(field));
                    }
                    for (const auto& key : request.Keys) {
                        columnsRequestedForKey[key] |= commonColumnsBitmap;
                    }
                }

                THashMap<TDynBitMap, std::vector<TUnversionedRow>> requestKeysByColumnSet;

                for (const auto& [key, columnsBitmap]: columnsRequestedForKey) {
                    auto& keys = requestKeysByColumnSet[columnsBitmap];
                    keys.push_back(key);
                    if (std::ssize(keys) >= maxKeysPerLookupRequest) {
                        context.OptimizedRequests.emplace_back(std::move(keys), GetNonZeroBits(columnsBitmap));
                        keys.clear();
                    }
                }

                for (auto& [columnsBitmap, keys]: requestKeysByColumnSet) {
                    if (!keys.empty()) {
                        context.OptimizedRequests.emplace_back(std::move(keys), GetNonZeroBits(columnsBitmap));
                    }
                }
            }

            template <COneOf<TVersionedLookupContext::TOptimizedRequest,
                TUnversionedLookupContext::TOptimizedRequest> TOptimizedRequest>
            void FillLookupBaseOptions(
                TLookupRowsOptionsBase& options,
                const TOptimizedRequest& optimizedRequest)
            {
                options.Timestamp = Owner_->Transaction_->StartTimestamp_;
                options.Timeout = Owner_->Transaction_->RequestTimeoutRemaining_;
                options.KeepMissingRows = true;
                // YT-15516: Workaround inconsistency between RPC proxy and native clients.
                YT_VERIFY(!optimizedRequest.Fields.empty());
                options.ColumnFilter = TColumnFilter(optimizedRequest.Fields);
                // This option is applicable only for chaos, others silently ignore it.
                options.ReplicaConsistency = EReplicaConsistency::Sync;
            }

            template <CWithAnyBoolArg<TLookupContext> TContext>
            TFuture<void> ExecuteRequestsForTable(TContext& context)
            {
                auto* transaction = Owner_->Transaction_;

                PrepareOptimizedRequests(context, transaction->Config_->MaxKeysPerLookupRequest);

                std::vector<TFuture<void>> asyncResults;
                asyncResults.reserve(context.OptimizedRequests.size());

                for (auto& optimizedRequest : context.OptimizedRequests) {
                    YT_LOG_DEBUG("Executing optimized lookup request (%v)",
                        context.FormatOptimizedRequest(
                            optimizedRequest,
                            transaction->Bootstrap_->GetInitialConfig()->LogCountLimitForPluralRequests,
                            transaction->TotalPerformanceStatistics_.ReadPhaseCount));

                    if (Y_UNLIKELY(FailLookups_)) {
                        optimizedRequest.AsyncResult = MakeFuture<typename TContext::TDBRequestResult>(TError(
                            "Failed to lookup due to testing storage options"));
                    } else {
                        if constexpr (std::same_as<TContext, TVersionedLookupContext>) {
                            TVersionedLookupRowsOptions options;
                            FillLookupBaseOptions(options, optimizedRequest);
                            options.RetentionConfig = transaction->SingleVersionRetentionConfig_;

                            optimizedRequest.AsyncResult = transaction->Client_->VersionedLookupRows(
                                context.TablePath,
                                context.NameTable.GetUnderlying(),
                                MakeSharedRange(optimizedRequest.Keys, Owner_->GetRowBuffer()),
                                std::move(options));
                        } else {
                            TLookupRowsOptions options;
                            FillLookupBaseOptions(options, optimizedRequest);
                            optimizedRequest.AsyncResult = transaction->Client_->LookupRows(
                                context.TablePath,
                                context.NameTable.GetUnderlying(),
                                MakeSharedRange(optimizedRequest.Keys, Owner_->GetRowBuffer()),
                                std::move(options));
                        }
                    }
                    optimizedRequest.AsyncResult = optimizedRequest.AsyncResult.Apply(
                        BIND([] (const TErrorOr<typename TContext::TDBRequestResult>& resultOrError) {
                            THROW_ERROR_EXCEPTION_IF_FAILED(resultOrError, "Error fetching data");
                            return resultOrError.Value();
                        }));
                    asyncResults.push_back(optimizedRequest.AsyncResult.template As<void>());
                }

                return AllSucceeded(std::move(asyncResults));
            }

            void ParseVersionedResultForTable(TVersionedLookupContext& context)
            {
                YT_LOG_DEBUG("Parsing versioned lookup session results (Path: %v)", context.TablePath);

                int aliveRowCount = 0;
                int missingRowCount = 0;
                int deadRowCount = 0;
                for (const auto& optimizedRequest : context.OptimizedRequests) {
                    YT_VERIFY(optimizedRequest.AsyncResult.IsSet());
                    const auto& result = optimizedRequest.AsyncResult.Get().Value();

                    auto rows = result.Rowset->GetRows();
                    YT_VERIFY(rows.Size() == optimizedRequest.Keys.size());

                    for (int keyIndex = 0; keyIndex < std::ssize(optimizedRequest.Keys); ++keyIndex) {
                        auto key = optimizedRequest.Keys[keyIndex];
                        auto createTag = [&] {
                            return context.FormatOptimizedRequestForKey(optimizedRequest, key);
                        };

                        auto row = rows[keyIndex];
                        if (!row) {
                            missingRowCount += 1;
                            YT_LOG_DEBUG("Got missing versioned lookup row (%v)", createTag());
                            continue;
                        }

                        // TODO(kmokrov): Remove check for deadRowCount after 24.1 release (YTADMINREQ-42981)
                        // and merge Parse.*ResultForTable functions
                        auto maxWriteTimestamp =
                            row.GetWriteTimestampCount() == 0
                            ? MinTimestamp
                            : row.WriteTimestamps()[0];

                        auto maxDeleteTimestamp =
                            row.GetDeleteTimestampCount() == 0
                            ? MinTimestamp
                            : row.DeleteTimestamps()[0];

                        if (maxWriteTimestamp <= maxDeleteTimestamp) {
                            deadRowCount += 1;
                            YT_LOG_DEBUG("Got dead lookup row (%v, Row: %v)",
                                createTag(),
                                ConvertVersionedRowToYsonString(row, [&] (const TVersionedValue& value) {
                                    auto fieldId = optimizedRequest.GetFieldIdChecked(value.Id);
                                    const auto* field = context.NameTable.GetFieldById(fieldId);
                                    return !field->Secure;
                                }));
                            continue;
                        }

                        auto& rowResult = GetOrInsert(context.Results, key, [&] {
                            return TVersionedLookupContext::TRowResult(context.NameTable.GetSize(), std::nullopt);
                        });

                        for (const auto& value : row.Values()) {
                            // Value.Id is actually a position in the rectangle request column filter.
                            auto fieldId = optimizedRequest.GetFieldIdChecked(value.Id);
                            // NB! Only initialize result with last written value.
                            if (!rowResult[fieldId]) {
                                rowResult[fieldId] = value;
                            }
                        }

                        aliveRowCount += 1;

                        YT_LOG_DEBUG("Got versioned lookup row (%v, Timestamps: %v)",
                            createTag(),
                            ConvertVersionedRowTimestampsToYsonString(row));
                    }
                }

                // These statistics is handful in case of logging suppression.
                YT_LOG_DEBUG("Versioned lookup session results parsed "
                    "(Path: %v, AliveRowCount: %v, MissingRowCount: %v, DeadRowCount: %v)",
                    context.TablePath,
                    aliveRowCount,
                    missingRowCount,
                    deadRowCount);
            }

            void ParseUnversionedResultForTable(TUnversionedLookupContext& context)
            {
                YT_LOG_DEBUG("Parsing unversioned lookup session results (Path: %v)", context.TablePath);

                int aliveRowCount = 0;
                int missingRowCount = 0;
                for (const auto& optimizedRequest : context.OptimizedRequests) {
                    YT_VERIFY(optimizedRequest.AsyncResult.IsSet());
                    const auto& result = optimizedRequest.AsyncResult.Get().Value();

                    auto rows = result.Rowset->GetRows();
                    YT_VERIFY(rows.Size() == optimizedRequest.Keys.size());

                    for (int keyIndex = 0; keyIndex < std::ssize(optimizedRequest.Keys); ++keyIndex) {
                        auto key = optimizedRequest.Keys[keyIndex];
                        auto createTag = [&] {
                            return context.FormatOptimizedRequestForKey(optimizedRequest, key);
                        };

                        auto row = rows[keyIndex];

                        if (!row) {
                            missingRowCount += 1;
                            YT_LOG_DEBUG("Got missing lookup row (%v)", createTag());
                            continue;
                        }

                        auto& rowResult = GetOrInsert(context.Results, key, [&] {
                            return TUnversionedLookupContext::TRowResult(context.NameTable.GetSize(), std::nullopt);
                        });

                        // NB: Fill rowResult not only for values but also for key columns.
                        for (const auto& value : row.Elements()) {
                            // Value.Id is actually a position in the rectangle request column filter.
                            auto fieldId = optimizedRequest.GetFieldIdChecked(value.Id);
                            // NB! Only initialize result with last written value.
                            if (!rowResult[fieldId]) {
                                rowResult[fieldId] = value;
                            }
                        }

                        aliveRowCount += 1;

                        YT_LOG_DEBUG("Got lookup row (%v)", createTag());
                    }
                }

                // These statistics is handful in case of logging suppression.
                YT_LOG_DEBUG("Unversioned lookup session results parsed "
                    "(Path: %v, AliveRowCount: %v, MissingRowCount: %v)",
                    context.TablePath,
                    aliveRowCount,
                    missingRowCount);
            }

            template <CWithAnyBoolArg<TLookupContext> TContext>
            std::vector<TError> HandleResultForTable(const TContext& context) const
            {
                auto formRequestResult = [&] (const TUnversionedRow& key, const TOptimalVector<const TDBField*>& fields)
                    -> std::optional<TOptimalVector<typename TContext::THandledValue>>
                {
                    auto resultIt = context.Results.find(key);
                    if (resultIt == context.Results.end()) {
                        return std::nullopt;
                    }

                    TOptimalVector<typename TContext::THandledValue> result;
                    result.reserve(fields.size());

                    const auto& rowResult = resultIt->second;
                    for (const auto* field : fields) {
                        auto fieldId = context.NameTable.GetIdByField(field);
                        const auto& optionalValue = rowResult[fieldId];
                        if (optionalValue) {
                            result.push_back(*optionalValue);
                        } else {
                            if constexpr (std::same_as<TContext, TVersionedLookupContext>) {
                                static const auto nullValue =
                                    MakeVersionedSentinelValue(EValueType::Null, NullTimestamp);
                                result.push_back(nullValue);
                            } else {
                                static const auto nullValue = MakeUnversionedNullValue();
                                result.push_back(nullValue);
                            }

                        }
                    }
                    return result;
                };

                std::vector<TError> errors;
                for (const auto& request: context.Requests) {
                    try {
                        auto valueToHandle = formRequestResult(request.Key, request.Fields);
                        if (valueToHandle) {
                            request.Handler(TRange(*valueToHandle));
                        } else {
                            request.Handler(std::nullopt);
                        }
                    } catch (const std::exception& ex) {
                        errors.emplace_back(ex);
                    }
                }

                // Similar to SelectRows, we do not add empty rows when there's no result.
                // Missed rows can be inferred via bitmap collected in successfullyReadRows.
                for (const auto& request: context.MultikeyRequests) {
                    try {
                        std::vector<typename TContext::TRow> rows;
                        TDynBitMap successfullyReadRows;
                        for (int keyIndex = 0; keyIndex < std::ssize(request.Keys); ++keyIndex) {
                            auto& key = request.Keys[keyIndex];
                            if (auto row = formRequestResult(key, request.Fields)) {
                                successfullyReadRows.Set(keyIndex);
                                if constexpr (std::same_as<TContext, TVersionedLookupContext>) {
                                    TVersionedRowBuilder builder(Owner_->GetRowBuffer(), /* compaction */ false);
                                    for (auto& column : key) {
                                        builder.AddKey(column);
                                    }
                                    for (auto& column : *row) {
                                        builder.AddValue(column);
                                    }
                                    rows.push_back(builder.FinishRow());
                                } else {
                                    TUnversionedRowBuilder builder;
                                    for (auto& column : *row) {
                                        builder.AddValue(column);
                                    }
                                    rows.push_back(Owner_->GetRowBuffer()->CaptureRow(
                                        builder.GetRow(),
                                        /* captureValues */ true));
                                }
                            }
                        }
                        // NOTE: Handler may keep a copy of rows for a while,
                        // preventing the deletion of entire RowBuffer_.
                        request.Handler(
                            MakeSharedRange(std::move(rows), Owner_->GetRowBuffer()),
                            successfullyReadRows);
                    } catch (const std::exception& ex) {
                        errors.emplace_back(ex);
                    }
                }

                return errors;
            }

            std::vector<TFuture<void>> DoExecuteRequests() override
            {
                std::vector<TFuture<void>> result;
                result.reserve(TableToVersionedLookupContext_.size() + TableToUnversionedLookupContext_.size());

                for (auto& [_, context]: TableToVersionedLookupContext_) {
                    result.push_back(ExecuteRequestsForTable(context));
                }
                for (auto& [_, context]: TableToUnversionedLookupContext_) {
                    result.push_back(ExecuteRequestsForTable(context));
                }

                return result;
            }

            void DoParseResult() override
            {
                for (auto& [_, context]: TableToVersionedLookupContext_) {
                    ParseVersionedResultForTable(context);
                }
                for (auto& [_, context]: TableToUnversionedLookupContext_) {
                    ParseUnversionedResultForTable(context);
                }
            }

            std::vector<TError> DoHandleResult() const override
            {
                std::vector<TError> errors;
                for (const auto& [_, context]: TableToVersionedLookupContext_) {
                    std::ranges::move(HandleResultForTable(context), std::back_inserter(errors));
                }
                for (const auto& [_, context]: TableToUnversionedLookupContext_) {
                    std::ranges::move(HandleResultForTable(context), std::back_inserter(errors));
                }
                return errors;
            }
        }; // class LookupRequestsProcessor

        class TSelectOrPullQueueProcessor
            : public TRequestsProcessor
        {
        protected:
            using TRequestsProcessor::TRequestsProcessor;

            void FillSelectRowsOptions(TSelectRowsOptions* options)
            {
                const auto* transaction = Owner_->Transaction_;

                options->Timestamp = transaction->StartTimestamp_;
                options->Timeout = transaction->RequestTimeoutRemaining_;
                options->InputRowLimit = transaction->Config_->InputRowLimit;
                options->OutputRowLimit = transaction->Config_->OutputRowLimit;
                options->MemoryLimitPerNode = transaction->Config_->SelectMemoryLimitPerNode;
                options->AllowFullScan = transaction->FullScanAllowed();
                // This option is applicable only for chaos, others silently ignore it.
                options->ReplicaConsistency = EReplicaConsistency::Sync;
                options->ExecutionPool = transaction->ExecutionPoolTag_;
            }
        }; // class TSelectOrPullQueueProcessor

        class TSelectRequestsProcessor final
            : public TSelectOrPullQueueProcessor
        {
        public:
            TSelectRequestsProcessor(TLoadContext* owner, bool failSelects):
                TSelectOrPullQueueProcessor(owner),
                FailSelects_(failSelects)
            { }

            void AddRequest(
                std::string query,
                NTableClient::EVersionedIOMode versionedIOMode,
                std::function<void(const IUnversionedRowsetPtr&)> handler)
            {
                Requests_.emplace_back(std::move(query), versionedIOMode, std::move(handler));
            }

            size_t Size() const override
            {
                return Requests_.size();
            }

        private:
            const bool FailSelects_;

            struct TRequest
            {
                std::string Query;
                NTableClient::EVersionedIOMode VersionedIOMode;
                std::function<void(const IUnversionedRowsetPtr&)> Handler;
                std::string Tag;
                TFuture<TSelectRowsResult> AsyncResult;
            };

            std::vector<TRequest> Requests_;

            std::vector<TFuture<void>> DoExecuteRequests() override
            {
                std::vector<TFuture<void>> result;
                result.reserve(Requests_.size());

                for (auto& request : Requests_) {
                    request.Tag = ToString(TGuid::Create());

                    YT_LOG_DEBUG("Executing select (Tag: %v, Query: %v, ReadPhaseIndex: %v)",
                        request.Tag,
                        request.Query,
                        Owner_->Transaction_->TotalPerformanceStatistics_.ReadPhaseCount);

                    if (Y_UNLIKELY(FailSelects_)) {
                        request.AsyncResult = MakeFuture<TSelectRowsResult>(
                            TError("Failed to select due to testing storage options"));
                    } else {
                        TSelectRowsOptions options;
                        FillSelectRowsOptions(&options);
                        options.VersionedReadOptions.ReadMode = request.VersionedIOMode;
                        // TODO: Migrate to std::string.
                        request.AsyncResult = Owner_->Transaction_->Client_->SelectRows(TString(request.Query), options);
                    }
                    result.push_back(request.AsyncResult.As<void>());
                }

                return result;
            }

            void DoParseResult() override
            { }

            std::vector<TError> DoHandleResult() const override
            {
                std::vector<TError> errors;

                for (const auto& request : Requests_) {
                    try {
                        const auto& result = request.AsyncResult.Get().Value();
                        YT_LOG_DEBUG("Got select results (Tag: %v, RowCount: %v, Statistics: %v)",
                            request.Tag,
                            result.Rowset->GetRows().Size(),
                            result.Statistics);
                        Owner_->Transaction_->CurrentPerformanceStatistics_
                            .SelectQueryStatistics[TryGetTableNameFromQuery(request.Query)]
                            .push_back(result.Statistics);
                        request.Handler(result.Rowset);
                    } catch (const std::exception& ex) {
                        errors.emplace_back(ex);
                    }
                }

                return errors;
            }
        }; // class TSelectRequestsProcessor

        class TPullQueueRequestsProcessor final
            : public TSelectOrPullQueueProcessor
        {
        public:
            using TSelectOrPullQueueProcessor::TSelectOrPullQueueProcessor;

            void AddRequest(
                const TDBTable* table,
                std::optional<i64> offset,
                int partitionIndex,
                std::optional<i64> rowCountLimit,
                std::optional<NYPath::TYPath> consumerPath,
                std::function<void(const NQueueClient::IQueueRowsetPtr&)> handler)
            {
                Requests_.emplace_back(
                    Owner_->GetTablePath(table),
                    offset,
                    partitionIndex,
                    rowCountLimit,
                    std::move(consumerPath),
                    std::move(handler));
            }

            size_t Size() const override
            {
                return Requests_.size();
            }

        private:
            struct TRequest
            {
                NYPath::TYPath Path;
                std::optional<i64> Offset;
                int PartitionIndex;
                std::optional<i64> RowCountLimit;
                std::optional<NYPath::TYPath> ConsumerPath;
                std::function<void(const NQueueClient::IQueueRowsetPtr&)> Handler;
                TString Tag;
                TFuture<NQueueClient::IQueueRowsetPtr> AsyncResult;
            };

            std::vector<TRequest> Requests_;

            std::vector<TFuture<void>> DoExecuteRequests() override
            {
                std::vector<TFuture<void>> result;
                result.reserve(Requests_.size());

                for (auto& request : Requests_) {
                    request.Tag = ToString(TGuid::Create());

                    YT_LOG_DEBUG("Executing pull queue ("
                        "Tag: %v, "
                        "Path: %v, "
                        "PartitionIndex: %v, "
                        "Offset: %v, "
                        "RowCountLimit: %v, "
                        "ConsumerPath: %v, "
                        "ReadPhaseIndex: %v)",
                        request.Tag,
                        request.Path,
                        request.PartitionIndex,
                        request.Offset,
                        request.RowCountLimit,
                        request.ConsumerPath,
                        Owner_->Transaction_->TotalPerformanceStatistics_.ReadPhaseCount);

                    NQueueClient::TQueueRowBatchReadOptions batchedReadOptions;
                    if (request.RowCountLimit) {
                        batchedReadOptions.MaxRowCount = request.RowCountLimit.value();
                    }

                    if (request.ConsumerPath) {
                        TPullQueueConsumerOptions options;
                        FillSelectRowsOptions(&options);
                        request.AsyncResult = Owner_->Transaction_->Client_->PullQueueConsumer(
                            *request.ConsumerPath,
                            request.Path,
                            request.Offset,
                            request.PartitionIndex,
                            batchedReadOptions,
                            options);
                    } else {
                        YT_VERIFY(request.Offset);
                        TPullQueueOptions options;
                        FillSelectRowsOptions(&options);
                        request.AsyncResult = Owner_->Transaction_->Client_->PullQueue(
                            request.Path,
                            *request.Offset,
                            request.PartitionIndex,
                            batchedReadOptions,
                            options);
                    }

                    result.push_back(request.AsyncResult.As<void>());
                }

                return result;
            }

            void DoParseResult() override
            { }

            std::vector<TError> DoHandleResult() const override
            {
                std::vector<TError> errors;

                for (const auto& request : Requests_) {
                    try {
                        const auto& result = request.AsyncResult.Get().Value();
                        YT_LOG_DEBUG("Got pull queue results (Tag: %v, RowCount: %v, StartOffset: %v)",
                            request.Tag,
                            result->GetRows().Size(),
                            result->GetStartOffset());
                        request.Handler(result);
                    } catch (const std::exception& ex) {
                        errors.emplace_back(ex);
                    }
                }

                return errors;
            }
        }; // class TPullQueueRequestsProcessor

        TLookupRequestsProcessor Lookups_;

        TSelectRequestsProcessor Selects_;

        TPullQueueRequestsProcessor PullQueues_;

    }; // class TLoadContext

    class TStoreContext
        : public TPersistenceContextBase
        , public IStoreContext
    {
    public:
        explicit TStoreContext(TTransaction::TImpl* transaction)
            : TPersistenceContextBase(transaction)
        { }

        const TRowBufferPtr& GetRowBuffer() override
        {
            return RowBuffer_;
        }

        void WriteRow(
            const TDBTable* table,
            const TObjectKey& tableKey,
            TRange<const TDBField*> fields,
            TUnversionedValueRange values,
            bool sharedWrite) override
        {
            YT_VERIFY(tableKey.size() == table->GetKeyFields(/*filterEvaluatedFields*/ true).size());
            YT_VERIFY(fields.Size() == values.Size());
            Requests_[table].push_back(TWriteRequest{
                .Key = CaptureKey(tableKey),
                .Fields = TCompactVector<const TDBField*, 4>(fields.begin(), fields.end()),
                .Values = TCompactVector<TUnversionedValue, 4>(values.begin(), values.end()),
                .SharedWrite = sharedWrite,
            });
        }

        void LockRow(
            const TDBTable* table,
            const TObjectKey& tableKey,
            TRange<const TDBField*> fields,
            NTableClient::ELockType lockType) override
        {
            YT_VERIFY(tableKey.size() == table->GetKeyFields(/*filterEvaluatedFields*/ true).size());
            Requests_[table].push_back(TLockRequest{
                .Key = CaptureKey(tableKey),
                .Fields = TCompactVector<const TDBField*, 4>(fields.begin(), fields.end()),
                .LockType = lockType,
            });
        }

        void DeleteRow(
            const TDBTable* table,
            const TObjectKey& tableKey) override
        {
            YT_VERIFY(tableKey.size() == table->GetKeyFields(/*filterEvaluatedFields*/ true).size());
            Requests_[table].push_back(TDeleteRequest{
                .Key = CaptureKey(tableKey)
            });
        }

        void AdvanceConsumer(
            const TString& consumer,
            const TDBTable* queue,
            const std::optional<TString>& queueCluster,
            int partitionIndex,
            std::optional<i64> oldOffset,
            i64 newOffset) override
        {
            TRichYPath queuePath(GetTablePath(queue));
            if (queueCluster) {
                queuePath.SetCluster(*queueCluster);
            }

            AdvanceConsumerRequests_.push_back({
                .ConsumerPath = GetConsumerPath(consumer),
                .QueuePath = std::move(queuePath),
                .PartitionIndex = partitionIndex,
                .OldOffset = oldOffset,
                .NewOffset = newOffset,
            });
        }

        void FillTransaction() override
        {
            const auto& transaction = Transaction_->UnderlyingTransactionDescriptor_->Transaction;
            const auto& Logger = Transaction_->Logger;

            std::vector<TRowModification> modifications;

            THashMap<const TDBTable*, TTableSchemaPtr> tableToSchema;
            {
                std::vector<TFuture<std::pair<const TDBTable*, TTableSchemaPtr>>> getSchemaAsyncResults;
                for (const auto& pair : Requests_) {
                    const auto* table = pair.first;
                    const auto& requests = pair.second;
                    for (const auto& variantRequest : requests) {
                        bool needMountInfo = Visit(variantRequest,
                            [&] (const TWriteRequest& request) {
                                return request.SharedWrite;
                            },
                            [&] (const TDeleteRequest& /*request*/) {
                                return false;
                            },
                            [&] (const TLockRequest& /*request*/) {
                                return true;
                            });
                        if (needMountInfo) {
                            getSchemaAsyncResults.emplace_back(
                                GetTableMountInfo(
                                    Transaction_->Bootstrap_->GetYTConnector(),
                                    table)
                                .Apply(BIND([table] (const TErrorOr<NTabletClient::TTableMountInfoPtr>& tableMountInfo)
                                    {
                                        return std::pair(
                                            table,
                                            tableMountInfo
                                                .ValueOrThrow()
                                                ->Schemas[NTabletClient::ETableSchemaKind::Write]);
                                    })));
                            break;
                        }
                    }
                }
                auto schemaResults = WaitFor(AllSucceeded(std::move(getSchemaAsyncResults)))
                    .ValueOrThrow();
                tableToSchema.insert(
                    std::make_move_iterator(schemaResults.begin()),
                    std::make_move_iterator(schemaResults.end()));
            }

            i64 writeCount = 0;
            i64 deleteCount = 0;
            i64 lockCount = 0;

            for (const auto& pair : Requests_) {
                const auto* table = pair.first;
                const auto& requests = pair.second;
                const auto path = GetTablePath(table);

                const auto& tableKeyFields = table->GetKeyFields(/*filterEvaluatedFields*/ true);

                std::optional<TTableSchemaPtr> schema;
                {
                    auto it = tableToSchema.find(table);
                    if (it != tableToSchema.end()) {
                        schema.emplace(it->second);
                    }
                }

                TExtendedNameTable nameTable(table);
                for (const auto& variantRequest : requests) {
                    Visit(variantRequest,
                        [&] (const TWriteRequest& request) {
                            for (const auto* field : request.Fields) {
                                nameTable.GetIdByFieldOrRegister(field);
                            }
                        },
                        [&] (const TDeleteRequest& /*request*/) {
                            // Nothing to do: all key fields are already in the name table.
                        },
                        [&] (const TLockRequest& request) {
                            for (const auto* field : request.Fields) {
                                nameTable.GetIdByFieldOrRegister(field);
                            }
                        });
                }

                i64 tableWriteCount = 0;
                i64 tableDeleteCount = 0;
                i64 tableLockCount = 0;

                modifications.clear();
                modifications.reserve(requests.size());
                for (const auto& variantRequest : requests) {
                    Visit(variantRequest,
                        [&] (const TWriteRequest& request) {
                            auto row = RowBuffer_->AllocateUnversioned(
                                tableKeyFields.size() + request.Fields.size());
                            for (size_t index = 0; index < tableKeyFields.size(); ++index) {
                                row[index] = request.Key[index];
                                row[index].Id = index; // Implicit call to the name table.
                            }
                            for (size_t index = 0; index < request.Fields.size(); ++index) {
                                auto& value = row[index + tableKeyFields.size()];
                                value = request.Values[index];
                                value.Id = nameTable.GetIdByField(request.Fields[index]);
                            }
                            auto lockMask = [&] {
                                if (!request.SharedWrite) {
                                    return TLockMask();
                                }
                                YT_VERIFY(schema && *schema);
                                return GetLockMask(
                                    *schema,
                                    transaction->GetAtomicity() == NTransactionClient::EAtomicity::Full,
                                    request.Fields,
                                    ELockType::SharedWrite);
                            }();
                            tableWriteCount += 1;
                            YT_LOG_DEBUG(
                                "Executing write (Path: %v, Key: %v, Columns: %v, Values: %v, SharedWrite: %v)",
                                path,
                                MakeFormattableView(
                                    TRange(row.Begin(), row.Begin() + tableKeyFields.size()),
                                    [] (TStringBuilderBase* builder, const auto& value) {
                                        FormatValue(builder, value, TStringBuf());
                                    }),
                                MakeFormattableView(
                                    TRange(row.Begin() + tableKeyFields.size(), row.End()),
                                    [&] (TStringBuilderBase* builder, const auto& value) {
                                        FormatValue(builder, nameTable.GetFieldById(value.Id)->Name, TStringBuf());
                                    }),
                                MakeFormattableView(
                                    TRange(row.Begin() + tableKeyFields.size(), row.End()),
                                    [&] (TStringBuilderBase* builder, const auto& value) {
                                        const auto* field = nameTable.GetFieldById(value.Id);
                                        if (field->Secure) {
                                            builder->AppendFormat("#");
                                        } else {
                                            FormatValue(builder, value, TStringBuf());
                                        }
                                    }),
                                request.SharedWrite);
                            modifications.push_back({
                                .Type = request.SharedWrite
                                    ? ERowModificationType::WriteAndLock
                                    : ERowModificationType::Write,
                                .Row = row.ToTypeErasedRow(),
                                .Locks = std::move(lockMask)});
                        },
                        [&] (const TDeleteRequest& request) {
                            auto key = RowBuffer_->AllocateUnversioned(tableKeyFields.size());
                            for (size_t index = 0; index < tableKeyFields.size(); ++index) {
                                key[index] = request.Key[index];
                                key[index].Id = index; // Implicit call to the name table.
                            }
                            tableDeleteCount += 1;
                            YT_LOG_DEBUG("Executing delete (Path: %v, Key: %v)", path, key);
                            modifications.push_back({
                                .Type = ERowModificationType::Delete,
                                .Row = key.ToTypeErasedRow(),
                                .Locks = TLockMask()});
                        },
                        [&] (const TLockRequest& request) {
                            auto key = RowBuffer_->AllocateUnversioned(tableKeyFields.size());
                            for (size_t index = 0; index < tableKeyFields.size(); ++index) {
                                key[index] = request.Key[index];
                                key[index].Id = index; // Implicit call to the name table.
                            }
                            YT_VERIFY(schema && *schema);
                            auto lockMask = GetLockMask(
                                *schema,
                                transaction->GetAtomicity() == NTransactionClient::EAtomicity::Full,
                                request.Fields,
                                request.LockType);
                            tableLockCount += 1;
                            YT_LOG_DEBUG("Executing lock (Path: %v, Key: %v, Column: %v, LockType: %v)",
                                path,
                                key,
                                MakeFormattableView(
                                    TRange(request.Fields.begin(), request.Fields.end()),
                                    [&] (TStringBuilderBase* builder, const auto& value) {
                                        FormatValue(builder, value->Name, TStringBuf());
                                    }),
                                request.LockType);
                            modifications.push_back(TRowModification{
                                .Type = ERowModificationType::WriteAndLock,
                                .Row = key.ToTypeErasedRow(),
                                .Locks = std::move(lockMask)});
                        });
                }

                // Log per request, per table and total counters for a client to decide granularity of logging via suppressions.
                YT_LOG_DEBUG("Executing table modifications (Path: %v, WriteCount: %v, DeleteCount: %v, LockCount: %v)",
                    path,
                    tableWriteCount,
                    tableDeleteCount,
                    tableLockCount);

                writeCount += tableWriteCount;
                deleteCount += tableDeleteCount;
                lockCount += tableLockCount;

                transaction->ModifyRows(
                    path,
                    nameTable.GetUnderlying(),
                    MakeSharedRange(std::move(modifications), RowBuffer_));
            }

            i64 advanceConsumerCount = RunAdvanceConsumers();

            // Log per request, per table and total counters for a client to decide granularity of logging via suppressions.
            YT_LOG_DEBUG_IF(writeCount + deleteCount + lockCount + advanceConsumerCount > 0,
                "Executed modifications (WriteCount: %v, DeleteCount: %v, LockCount: %v, AdvanceConsumerCount: %v)",
                writeCount,
                deleteCount,
                lockCount,
                advanceConsumerCount);
        }

    private:
        struct TWriteRequest
        {
            TUnversionedRow Key;
            TCompactVector<const TDBField*, 4> Fields;
            TCompactVector<TUnversionedValue, 4> Values;
            bool SharedWrite;
        };

        struct TDeleteRequest
        {
            TUnversionedRow Key;
        };

        struct TLockRequest
        {
            TUnversionedRow Key;
            TCompactVector<const TDBField*, 4> Fields;
            ELockType LockType;
        };

        using TRequest = std::variant<TWriteRequest, TDeleteRequest, TLockRequest>;

        struct TAdvanceConsumerRequest
        {
            NYPath::TRichYPath ConsumerPath;
            NYPath::TRichYPath QueuePath;
            int PartitionIndex;
            std::optional<i64> OldOffset;
            i64 NewOffset;
        };

        THashMap<const TDBTable*, std::vector<TRequest>> Requests_;
        std::vector<TAdvanceConsumerRequest> AdvanceConsumerRequests_;

        TYPath GetTablePath(const TDBTable* table)
        {
            const auto& ytConnector = Transaction_->Bootstrap_->GetYTConnector();
            return ytConnector->GetTablePath(table);
        }

        TYPath GetConsumerPath(const TString& consumer)
        {
            const auto& ytConnector = Transaction_->Bootstrap_->GetYTConnector();
            return ytConnector->GetConsumerPath(consumer);
        }

        TLockMask GetLockMask(
            const TTableSchemaPtr& schema,
            bool fullAtomicity,
            const TCompactVector<const TDBField*, 4>& fields,
            ELockType lockType)
        {
            std::vector<TString> locks;
            locks.reserve(fields.size());
            for (const auto* field : fields) {
                auto column = schema->GetColumnOrThrow(field->Name);
                if (!column.Lock()) {
                    THROW_ERROR_EXCEPTION("Column %v lock group must be specified",
                        column.Name());
                }
                locks.push_back(*column.Lock());
            }
            std::sort(locks.begin(), locks.end());
            locks.erase(
                std::unique(locks.begin(), locks.end()),
                locks.end());
            YT_VERIFY(locks.size() <= TLegacyLockMask::MaxCount);

            return NTableClient::GetLockMask(
                *schema,
                fullAtomicity,
                locks,
                lockType);
        }

        i64 RunAdvanceConsumers()
        {
            const auto& transaction = Transaction_->UnderlyingTransactionDescriptor_->Transaction;
            for (auto& request : AdvanceConsumerRequests_) {
                transaction->AdvanceConsumer(
                    request.ConsumerPath,
                    request.QueuePath,
                    request.PartitionIndex,
                    request.OldOffset,
                    request.NewOffset);
            }

            return AdvanceConsumerRequests_.size();
        }
    }; // class TStoreContext

    TTransaction* const Owner_;
    NMaster::IBootstrap* const Bootstrap_;
    const TTransactionManagerConfigPtr Config_;
    const TTransactionId Id_;
    const TTimestamp StartTimestamp_;
    const IClientPtr Client_;
    const std::optional<TYTTransactionDescriptor> UnderlyingTransactionDescriptor_;

    const TLogger Logger;

    const TRetentionConfigPtr SingleVersionRetentionConfig_ = New<TRetentionConfig>();

    const TAsyncSemaphorePtr Semaphore_ = New<TAsyncSemaphore>(1);

    bool AccessControlPreloadEnabled_ = false;

    std::optional<TDuration> RequestTimeout_;
    std::optional<TDuration> RequestTimeoutRemaining_;

    std::atomic<ETransactionState> State_ = ETransactionState::Active;

    std::unique_ptr<ISession> Session_;

    TPerformanceStatistics TotalPerformanceStatistics_;
    TPerformanceStatistics CurrentPerformanceStatistics_;

    THashMap<std::pair<const TObject*, const TAttributeSchema*>, std::function<void()>> Validators_;

    THashMap<TCommitActionType, THashSet<TObject*>> CommitActions_;

    using TAction = TTransaction::TAction;
    std::deque<TAction> AfterCommitActions_;
    std::deque<TAction> AfterAbortActions_;

    TTimestamp CommitStartTimestamp_ = TTimestamp{0};

    TInstant StartTime_;
    TInstant CurrentRequestStartTime_;
    TInstant CommitStartTime_ = TInstant::Zero();
    TInstant CommitFinishTime_ = TInstant::Zero();

    i64 ReadPhaseLimit_;

    TReadingTransactionOptions ReadingOptions_;
    TMutatingTransactionOptions MutatingOptions_;

    const std::optional<TString> ExecutionPoolTag_;

    template <class F>
    auto AbortOnException(F func) -> decltype(func())
    {
        try {
            return func();
        } catch (const std::exception& ex) {
            // Fire-and-forget.
            YT_UNUSED_FUTURE(Abort());
            THROW_ERROR_EXCEPTION("Error executing transactional request; transaction aborted")
                << ex;
        }
    }

    bool IsReadWrite() const
    {
        return UnderlyingTransactionDescriptor_.has_value();
    }

    bool IsPrecommitted() const
    {
        return ETransactionState::Precommitted == State_.load();
    }

    TCreationAttributeMatches ParseCreationAttributes(
        const TCreateObjectSubrequest& request, const TObjectKey& key)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();

        auto matchedAttributes = MatchCreationAttributes(
            objectManager->GetTypeHandlerOrThrow(request.Type),
            request.Attributes);
        const auto& unmatchedMandatory = matchedAttributes.UnmatchedMandatoryAttributes;
        if (!unmatchedMandatory.empty()) {
            THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
                "Missing mandatory attributes %v when trying to create %v %Qv",
                MakeFormattableView(
                    unmatchedMandatory,
                    [] (TStringBuilderBase* builder, const auto& pair) {
                        const auto& [schema, path] = pair;
                        TYPath fullPath = schema->FormatPathEtc();
                        if (path) {
                            fullPath += "/" + path;
                        }
                        builder->AppendFormat("%Qv", std::move(fullPath));
                    }),
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(request.Type),
                key);
        }
        return matchedAttributes;
    }

    void ScheduleExistenceChecks(
        const std::vector<TCreateObjectSubrequest>& subrequests,
        const std::vector<TKeyAttributeMatches>& matchedAttributesVector)
    {
        TTraceContextGuard guard(CreateTraceContextFromCurrent("NYT::NOrm::TTransaction::ScheduleExistenceChecks"));

        YT_VERIFY(subrequests.size() == matchedAttributesVector.size());

        for (int index = 0; index < std::ssize(subrequests); ++index) {
            const auto& request = subrequests[index];
            if (request.UpdateIfExisting) {
                const auto& matchedAttributes = matchedAttributesVector[index];
                Session_->ScheduleExistenceCheck(
                    request.Type,
                    matchedAttributes.Key,
                    matchedAttributes.ParentKey);
            }
        }
    }

    void FillObjectPermissions(
        std::vector<NAccessControl::TObjectPermission>& objectPermissions,
        TObject* object,
        const std::vector<TUpdateRequest>& requests)
    {
        for (const auto& request : requests) {
            auto path = GetUpdateRequestPath(request);
            objectPermissions.push_back({
                .Object = object,
                .AttributePath = path,
                .Permission = GetPermissionForPathUpdate(path),
            });
        }
    }

    std::vector<TObject*> DoCreateObjects(
        std::vector<TCreateObjectSubrequest> subrequests,
        IUpdateContext* context,
        const TTransactionCallContext& transactionCallContext)
    {
        TTraceContextGuard guard(CreateTraceContextFromCurrent("NYT::NOrm::TTransaction::DoCreateObjects"));

        EnsureNotPrecommitted();

        const auto& objectManager = Bootstrap_->GetObjectManager();
        const auto& accessControlManager = Bootstrap_->GetAccessControlManager();

        auto matchedKeyAttributes = MatchAllKeyAttributes(objectManager, subrequests);
        ValidateUpdateIfExistingIds(objectManager, subrequests, matchedKeyAttributes);
        ScheduleExistenceChecks(subrequests, matchedKeyAttributes);
        PerformExistenceChecksAndSchedulePermissionsValidation(
            context,
            accessControlManager,
            objectManager,
            subrequests,
            matchedKeyAttributes);

        // Create/update/another prefetch pass.
        auto prefetchPassResult = RunCreateObjectsPrefetchPass(
            context,
            transactionCallContext,
            subrequests,
            matchedKeyAttributes);

        // Apply attributes pass.
        RunCreateObjectsApplyAttributesPass(
            context,
            transactionCallContext,
            prefetchPassResult.CreatedObjects,
            matchedKeyAttributes);

        return std::move(prefetchPassResult.Objects);
    }

    void ValidateUpdateRequest(const TSetUpdateRequest& request) const
    {
        if (GetConfig()->ForbidYsonAttributesUsage) {
            NYPath::TTokenizer tokenizer(request.Path);
            for (auto type = tokenizer.GetType(); type != NYPath::ETokenType::EndOfStream; type = tokenizer.Advance()) {
                THROW_ERROR_EXCEPTION_IF(type == NYPath::ETokenType::At,
                    "Path %Qv containing YSON-attributes is forbidden to be updated from user queries",
                    request.Path);
            }

            auto detectingConsumer = NAttributes::CreateAttributesDetectingConsumer([&request] {
                THROW_ERROR_EXCEPTION("Cannot update %v with user payload containing YSON-attributes", request.Path);
            });
            Serialize(request.Value, detectingConsumer.get());
        }
    }

    void PopulateUpdateRequestsFromAttributes(
        const INodePtr& attributes,
        std::vector<TUpdateRequest>& updateRequests)
    {
        for (auto& updateRequest : updateRequests) {
            auto* setRequest = std::get_if<TSetUpdateRequest>(&updateRequest);
            if (!setRequest) {
                continue;
            }
            if (setRequest->Value && setRequest->Value->GetType() != NYTree::ENodeType::Entity) {
                continue;
            }
            setRequest->Value =
                NAttributes::GetNodeByPathOrEntity(attributes, setRequest->Path);
        }
    }

    void DoUpdateObject(
        TObject* object,
        const std::vector<TUpdateRequest>& requests,
        const std::vector<TAttributeTimestampPrerequisite>& prerequisites,
        IUpdateContext* context,
        const TTransactionCallContext& transactionCallContext,
        IAttributeValuesConsumer* consumer)
    {
        object->ValidateExists();
        // Later used in FlushTransaction, where history and watching rely on it.
        object->ScheduleMetaResponseLoad();

        std::vector<TAttributeUpdateMatch> matches;
        matches.reserve(requests.size() + (Config_->AttributePrerequisiteReadLockEnabled ? prerequisites.size() : 0));

        for (const auto& request : requests) {
            MatchAttributeUpdate(object, request, &matches);
        }
        if (Config_->AttributePrerequisiteReadLockEnabled) {
            for (const auto& prerequisite : prerequisites) {
                TLockUpdateRequest prerequisitesReadLock{
                    .Path = prerequisite.Path,
                    .LockType = ELockType::SharedStrong,
                };
                MatchAttributeUpdate(object, prerequisitesReadLock, &matches);
            }
        }

        for (const auto& match : matches) {
            PreloadAttributeUpdate(Owner_, object, match);
        }

        for (const auto& prerequisite : prerequisites) {
            auto resolveResult = ResolveAttributeValidated(
                object->GetTypeHandler(),
                prerequisite.Path);
            PrefetchAttributeTimestamp(resolveResult, object);
            context->AddPreparer([=, resolveResult = std::move(resolveResult)] (IUpdateContext* /*context*/) {
                auto actualTimestamp = FetchAttributeTimestamp(resolveResult, object);
                if (actualTimestamp > prerequisite.Timestamp) {
                    THROW_ERROR_EXCEPTION(NClient::EErrorCode::PrerequisiteCheckFailure,
                        "Prerequisite timestamp check failed for attribute %Qv of %v: "
                        "expected <=%v, actual %v",
                        resolveResult.Attribute->FormatPathEtc(),
                        object->GetDisplayName(/*keyOnly*/ true),
                        prerequisite.Timestamp,
                        actualTimestamp);
                }
            });
        }

        if (consumer) {
            const auto* type = Bootstrap_->GetObjectManager()
                ->GetTypeHandlerOrCrash(object->GetType())
                ->GetRootProtobufType();
            consumer->Initialize(type);
        }

        THashSet<const TAttributeSchema*> updatedAttributes;
        for (const auto& match : matches) {
            context->AddPreparer([=, this] (IUpdateContext* /*context*/) {
                ApplyAttributeUpdate(Owner_, object, match, transactionCallContext, consumer);
            });

            const TAttributeSchema* current = match.Schema;
            while (current && updatedAttributes.insert(current).second) {
                current = current->GetParent();
            }
        }

        if (consumer) {
            consumer->Finalize({});
        }

        OnAttributesUpdated(object, updatedAttributes, context);
    }

    void OnAttributesUpdated(
        TObject* object,
        const THashSet<const TAttributeSchema*>& attributes,
        IUpdateContext* context)
    {
        for (const auto* schema : attributes) {
            schema->RunUpdatePrehandlers(Owner_, object);

            schema->RunUpdateHandlers(Owner_, object, context);

            Validators_.emplace(std::pair(object, schema), [object, schema, this] {
                schema->RunValidators(Owner_, object);
            });
        }
    }

    static TUpdateRequest OverwriteRequestPath(
        const TUpdateRequest& request,
        const TYPath& path)
    {
        return Visit(request,
            [&] (auto typedRequest) -> TUpdateRequest {
                typedRequest.Path = path;
                return typedRequest;
            });
    }

    void ValidateObjectUpdateRequest(
        TObject* object,
        const TUpdateRequest& request)
    {
        YT_VERIFY(!object->RemovalStarted() || object->IsFinalized());
        if (object->IsFinalized()) {
            auto path = GetUpdateRequestPath(request);
            bool pathAllowed = HasPrefix(path, FinalizersAttributePath) ||
                path == FinalizationTimeAttributePath ||
                path == AddFinalizerControlPath ||
                path == CompleteFinalizerControlPath;
            THROW_ERROR_EXCEPTION_UNLESS(pathAllowed,
                NClient::EErrorCode::InvalidObjectState,
                "Trying to update %Qv, but %v is already in %Qlv state and cannot be modified",
                path,
                object->GetDisplayName(),
                object->GetState());
        }
    }

    void ValidateAttributeUpdateMatch(
        TObject* object,
        const TAttributeUpdateMatch& match)
    {
        auto requestPath = GetUpdateRequestPath(match.Request);

        if (match.Schema->IsSuperuserOnlyUpdatable(requestPath)) {
            Bootstrap_->GetAccessControlManager()->ValidateSuperuser(Format(
                "update attribute %Qv at path %Qv",
                match.Schema->FormatPathEtc(),
                requestPath));
        } else if (!match.Schema->IsUpdatable(requestPath)) {
            THROW_ERROR_EXCEPTION("Attribute %Qv does not support updates at path %Qv",
                match.Schema->FormatPathEtc(),
                requestPath);
        }

        // In case of empty suffix path there is no need to load attribute and validate read permissions accordingly.
        if (requestPath) {
            const auto& accessControlManager = Bootstrap_->GetAccessControlManager();

            const TAttributeSchema* current = match.Schema;
            while (current) {
                if (auto readPermission = current->GetReadPermission(); readPermission != TAccessControlPermissionValues::None) {
                    accessControlManager->ValidatePermission(object, readPermission);
                }
                current = current->GetParent();
            }
        }
    }

    template <CInvocable<void(const TScalarAttributeSchema*, TUpdateRequest)> TOnLeafAttribute>
    void ForEachSetUpdateLeafAttribute(
        const TAttributeSchema* schema,
        TSetUpdateRequest&& request,
        const TOnLeafAttribute& onLeafAttribute,
        bool calledFromRoot = true)
    {
        if (!calledFromRoot && schema->IsOpaqueForUpdates()) {
            return;
        }
        if (auto* scalarSchema = schema->TryAsScalar()) {
            ValidateUpdateRequest(request);
            onLeafAttribute(scalarSchema, std::move(request));
            return;
        }

        auto* compositeSchema = schema->TryAsComposite();
        YT_VERIFY(compositeSchema);
        YT_VERIFY(request.Path.Empty());

        THROW_ERROR_EXCEPTION_IF(request.Value->GetType() != ENodeType::Map,
            "Attribute %Qv cannot be updated from values of type %Qlv",
            compositeSchema->FormatPathEtc(),
            request.Value->GetType());

        auto mapValue = request.Value->AsMap();
        auto* defaultEtcChild = compositeSchema->GetEtcChild();
        TCompactFlatMap<const TAttributeSchema*, IMapNodePtr, 4> etcChildrenMapValues;

        auto updateMode = compositeSchema->GetUpdateMode().value_or(Bootstrap_->GetDBConfig().UpdateObjectMode);
        if (updateMode == ESetUpdateObjectMode::Overwrite) {
            for (const auto& [key, child] : compositeSchema->KeyToChild()) {
                if (!mapValue->FindChild(key)) {
                    ForEachSetUpdateLeafAttribute(
                        child,
                        TSetUpdateRequest{
                            .Path = TYPath(),
                            .Value = GetAttributeDefaultValue(child),
                            .SharedWrite = request.SharedWrite,
                            .AggregateMode = request.AggregateMode},
                        onLeafAttribute,
                        /*calledFromRoot*/ false);
                }
            }
            for (const auto& [_, etcChild] : compositeSchema->NameToEtcChild()) {
                etcChildrenMapValues[etcChild] = GetEphemeralNodeFactory()->CreateMap();
            }
        }

        for (auto children = mapValue->GetChildren(); const auto& [key, childValue] : children) {
            auto* child = compositeSchema->FindChild(key);
            auto* etcChild = compositeSchema->FindEtcChildByFieldName(key);
            if (child) {
                ForEachSetUpdateLeafAttribute(
                    child,
                    TSetUpdateRequest{
                        .Path = TYPath(),
                        .Value = std::move(childValue),
                        .SharedWrite = request.SharedWrite,
                        .AggregateMode = request.AggregateMode},
                    onLeafAttribute,
                    /*calledFromRoot*/ false);
            } else if (etcChild || defaultEtcChild) {
                if (!etcChild) {
                    etcChild = defaultEtcChild;
                }
                auto& etcMapValue = etcChildrenMapValues[etcChild];
                if (!etcMapValue) {
                    etcMapValue = GetEphemeralNodeFactory()->CreateMap();
                }
                mapValue->RemoveChild(childValue);
                etcMapValue->AddChild(key, childValue);
            } else {
                THROW_ERROR_EXCEPTION("Attribute %Qv has no child with key %Qv",
                    schema->FormatPathEtc(),
                    key);
            }
        }

        for (auto& [etcChild, etcMapValue] : etcChildrenMapValues) {
            ForEachSetUpdateLeafAttribute(
                etcChild,
                TSetUpdateRequest{
                    .Path = TYPath(),
                    .Value = std::move(etcMapValue),
                    .SharedWrite = request.SharedWrite,
                    .AggregateMode = request.AggregateMode},
                onLeafAttribute,
                /*calledFromRoot*/ false);
        }
    }

    void MatchAttributeUpdate(
        TObject* object,
        const TUpdateRequest& request,
        std::vector<TAttributeUpdateMatch>* matches)
    {
        ValidateObjectUpdateRequest(object, request);
        auto resolveResult = ResolveAttribute(
            object->GetTypeHandler(),
            GetUpdateRequestPath(request),
            /*callback*/ {});

        const auto* resolvedSchema = resolveResult.Attribute;

        if (!(resolvedSchema->TryAsComposite() || resolvedSchema->TryAsScalar()->HasValueSetter())) {
            THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
                "Attribute %Qv does not support updates",
                resolvedSchema->GetPath())
                << TErrorAttribute("suffix_path", resolveResult.SuffixPath);
        }

        auto addMatch = [&, this] (const TScalarAttributeSchema* schema, TUpdateRequest request) {
            if (!schema->HasValueSetter()) {
                return;
            }

            TAttributeUpdateMatch match{
                schema,
                std::move(request)};
            ValidateAttributeUpdateMatch(object, match);
            matches->push_back(std::move(match));
        };

        auto addMethodMatch = [&, this] (const TScalarAttributeSchema* schema, TUpdateRequest request) {
            if (!schema->HasMethod()) {
                THROW_ERROR_EXCEPTION("Attribute %Qv does not support method call",
                    schema->FormatPathEtc());
            }

            TAttributeUpdateMatch match{
                schema,
                std::move(request)};
            ValidateAttributeUpdateMatch(object, match);
            matches->push_back(std::move(match));
        };

        auto checkOptionsRestrictions = [resolvedSchema] (const auto& typedRequest, TStringBuf action) {
            auto checkOption = [&] (TStringBuf option) {
                THROW_ERROR_EXCEPTION_IF(resolvedSchema->TryAsComposite(),
                    NClient::EErrorCode::InvalidRequestArguments,
                    "Composite attribute %Qv cannot be %v with %v", resolvedSchema->GetPath(), action, option);
                THROW_ERROR_EXCEPTION_IF(resolvedSchema->TryAsScalar()->IsEtc(),
                    NClient::EErrorCode::InvalidRequestArguments,
                    "Etc attribute %Qv cannot be %v with %v", resolvedSchema->GetPath(), action, option);
                THROW_ERROR_EXCEPTION_IF(!typedRequest.Path.empty(),
                    NClient::EErrorCode::InvalidRequestArguments,
                    "Attribute %Qv cannot be %v with %v and nonempty suffix path %Qv",
                    resolvedSchema->GetPath(),
                    action,
                    option,
                    typedRequest.Path);
            };
            if (typedRequest.SharedWrite && *typedRequest.SharedWrite) {
                checkOption("shared write lock");
            }
            if (typedRequest.AggregateMode != EAggregateMode::Unspecified) {
                checkOption("aggregate mode");
            }
        };

        Visit(OverwriteRequestPath(request, resolveResult.SuffixPath),
            [&] (TSetUpdateRequest&& typedRequest) {
                checkOptionsRestrictions(typedRequest, "updated");
                ForEachSetUpdateLeafAttribute(
                    resolvedSchema,
                    std::move(typedRequest),
                    addMatch);
            },
            [&] (TRemoveUpdateRequest&& typedRequest) {
                resolvedSchema->ForEachLeafAttribute(
                    [&] (const TScalarAttributeSchema* schema) {
                        if (!schema->IsOpaqueForUpdates() || schema == resolvedSchema) {
                            addMatch(schema, std::move(typedRequest));
                        }
                        return false;
                    });
            },
            [&] (TLockUpdateRequest&& typedRequest) {
                resolvedSchema->ForEachLeafAttribute(
                    [&] (const TScalarAttributeSchema* schema) {
                        if (!schema->IsOpaqueForUpdates() || schema == resolvedSchema) {
                            addMatch(schema, std::move(typedRequest));
                        }
                        return false;
                    });
            },
            [&] (TMethodRequest&& typedRequest) {
                THROW_ERROR_EXCEPTION_IF(resolvedSchema->TryAsComposite(),
                    NClient::EErrorCode::InvalidRequestArguments,
                    "Composite attribute %Qv cannot be method",
                    resolvedSchema->GetPath());
                addMethodMatch(resolvedSchema->AsScalar(), std::move(typedRequest));
            });
    }

    void PreloadAttributeUpdate(
        TTransaction* transaction,
        TObject* object,
        const TAttributeUpdateMatch& match)
    {
        if (!match.Schema->HasUpdatePreloader()) {
            return;
        }

        YT_LOG_DEBUG("Preloading attribute update (ObjectKey: %v, Attribute: %v, Path: %v)",
            object->GetKey(),
            match.Schema->FormatPathEtc(),
            GetUpdateRequestPath(match.Request));

        match.Schema->RunUpdatePreloader(transaction, object, match.Request);
    }

    void ApplyAttributeUpdate(
        TTransaction* transaction,
        TObject* object,
        const TAttributeUpdateMatch& match,
        const TTransactionCallContext& transactionCallContext,
        IAttributeValuesConsumer* consumer)
    {
        const auto& request = match.Request;
        auto* scalarSchema = match.Schema->TryAsScalar();

        // Expect leaf attribute, because recursive matching has been performed previously.
        YT_VERIFY(scalarSchema);

        try {
            Visit(request,
                [&] (const TSetUpdateRequest& typedRequest) {
                    ApplyAttributeSetUpdate(
                        transaction,
                        object,
                        scalarSchema,
                        typedRequest,
                        transactionCallContext);
                },
                [&] (const TLockUpdateRequest& typedRequest) {
                    ApplyAttributeLockUpdate(
                        transaction,
                        object,
                        scalarSchema,
                        typedRequest);
                },
                [&] (const TRemoveUpdateRequest& typedRequest) {
                    ApplyAttributeRemoveUpdate(
                        transaction,
                        object,
                        scalarSchema,
                        typedRequest);
                },
                [&] (const TMethodRequest& typedRequest) {
                    ApplyAttributeMethod(
                        transaction,
                        object,
                        scalarSchema,
                        typedRequest,
                        consumer);
                });
        } catch (const std::exception& ex) {
            auto applying = Visit(request,
                [] (const TSetUpdateRequest&) {
                    return "setting";
                },
                [] (const TLockUpdateRequest&) {
                    return "locking";
                },
                [] (const TRemoveUpdateRequest&) {
                    return "removing";
                },
                [] (const TMethodRequest&) {
                    return "applying method on";
                });

            THROW_ERROR_EXCEPTION("Error %v attribute %Qv of %v",
                applying,
                match.Schema->FormatPathEtc() + GetUpdateRequestPath(request),
                object->GetDisplayName(/*keyOnly*/ true))
                << ex;
        }
    }

    void ApplyAttributeSetUpdate(
        TTransaction* transaction,
        TObject* object,
        const TScalarAttributeSchema* schema,
        const TSetUpdateRequest& request,
        const TTransactionCallContext& transactionCallContext)
    {
        if (!schema->HasValueSetter()) {
            THROW_ERROR_EXCEPTION("Attribute %Qv does not support set updates",
                schema->FormatPathEtc());
        }

        // "if" prevents logging of sensitive data.
        if (schema->GetReadPermission() == TAccessControlPermissionValues::None) {
            YT_LOG_DEBUG("Applying set update (ObjectKey: %v, Attribute: %v, Path: %v, Value: %v, Recursive: %v, "
                "SharedWrite: %v, AggregateMode: %v)",
                object->GetKey(),
                schema->FormatPathEtc(),
                request.Path,
                ConvertToYsonString(request.Value, EYsonFormat::Text),
                request.Recursive,
                request.SharedWrite,
                request.AggregateMode);
        } else {
            YT_LOG_DEBUG("Applying set update (ObjectKey: %v, Attribute: %v)",
                object->GetKey(),
                schema->FormatPathEtc());
        }

        schema->RunValueSetter(
            transaction,
            object,
            request.Path,
            request.Value,
            request.Recursive,
            request.SharedWrite,
            request.AggregateMode,
            transactionCallContext);
    }

    void ApplyAttributeLockUpdate(
        TTransaction* transaction,
        TObject* object,
        const TScalarAttributeSchema* schema,
        const TLockUpdateRequest& request)
    {
        if (!schema->HasLocker()) {
            THROW_ERROR_EXCEPTION("Attribute %Qv does not support lock updates",
                schema->FormatPathEtc());
        }

        YT_LOG_DEBUG("Applying lock (ObjectKey: %v, Attribute: %v, Path: %v, LockType: %v",
            object->GetKey(),
            schema->FormatPathEtc(),
            request.Path,
            request.LockType);

        schema->RunLocker(transaction, object, request.Path, request.LockType);
    }

    void ApplyAttributeRemoveUpdate(
        TTransaction* transaction,
        TObject* object,
        const TScalarAttributeSchema* schema,
        const TRemoveUpdateRequest& request)
    {
        if (!schema->HasRemover()) {
            THROW_ERROR_EXCEPTION("Attribute %Qv does not support remove updates",
                schema->FormatPathEtc());
        }

        YT_LOG_DEBUG("Applying remove update (ObjectKey: %v, Attribute: %v, Path: %v, Force: %v)",
            object->GetKey(),
            schema->FormatPathEtc(),
            request.Path,
            request.Force);

        schema->RunRemover(transaction, object, request.Path, request.Force);
    }

    void ApplyAttributeMethod(
        TTransaction* transaction,
        TObject* object,
        const TScalarAttributeSchema* schema,
        const TMethodRequest& request,
        IAttributeValuesConsumer* consumer)
    {
        THROW_ERROR_EXCEPTION_UNLESS(request.Path.empty(),
            "Method %Qv does not support suffix path",
            schema->FormatPathEtc());

        // "if" prevents logging of sensitive data.
        if (schema->GetReadPermission() == TAccessControlPermissionValues::None) {
            YT_LOG_DEBUG("Applying method (ObjectKey: %v, Attribute: %v, Path: %v, Value: %v)",
                object->GetKey(),
                schema->FormatPathEtc(),
                request.Path,
                ConvertToYsonString(request.Value, EYsonFormat::Text));
        } else {
            YT_LOG_DEBUG("Applying method (ObjectKey: %v, Attribute: %v)",
                object->GetKey(),
                schema->FormatPathEtc());
        }

        THROW_ERROR_EXCEPTION_UNLESS(consumer,
            "Empty consumer in method %Qv",
            schema->FormatPathEtc());
        auto ysonConsumer = consumer->OnValueBegin(schema->GetPath());
        schema->RunMethod(
            transaction,
            object,
            request.Path,
            request.Value,
            ysonConsumer);
        consumer->OnValueEnd();
    }

    static std::vector<IFetcherPtr> BuildAttributeFetchers(
        TFetcherContext* fetcherContext,
        const std::vector<TResolveAttributeResult>& resolveResults)
    {
        std::vector<IFetcherPtr> fetchers;
        fetchers.reserve(resolveResults.size());
        for (const auto& resolveResult : resolveResults) {
            fetchers.push_back(MakeSelectorFetcher(resolveResult, fetcherContext));
        }

        return fetchers;
    }

    IUnversionedRowsetPtr RunSelect(std::string queryString, std::source_location location)
    {
        IUnversionedRowsetPtr rowset;
        Session_->ScheduleLoad(
            [queryString = std::move(queryString), &rowset] (ILoadContext* context) {
                context->ScheduleSelect(
                    std::move(queryString),
                    [&rowset] (const IUnversionedRowsetPtr& selectedRowset) {
                        rowset = selectedRowset;
                    });
            });
        Session_->FlushLoads(location);
        return rowset;
    }

    void RunAfterCommitActions()
    {
        while (!AfterCommitActions_.empty()) {
            try {
                AfterCommitActions_.front()();
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Failed to run post-commit action");
            }
            AfterCommitActions_.pop_front();
        }
    }

    void RunAfterAbortActions()
    {
        while (!AfterAbortActions_.empty()) {
            try {
                AfterAbortActions_.front()();
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Failed to run post-abort action");
            }
            AfterAbortActions_.pop_front();
        }
    }

    void ValidateAccessControlPermissions(
        const TAccessControlManagerPtr& accessControlManager,
        const std::vector<TObjectUpdateRequest>& objectUpdateRequests)
    {
        TTraceContextGuard guard(
            CreateTraceContextFromCurrent("NYT::NOrm::TTransaction::ValidateAccessControlPermissions"));

        std::vector<NAccessControl::TObjectPermission> objectPermissions;
        size_t permissionCount = 0;
        for (const auto& request : objectUpdateRequests) {
            permissionCount += request.Updates.size();
        }
        objectPermissions.reserve(permissionCount);

        for (const auto& request : objectUpdateRequests) {
            FillObjectPermissions(objectPermissions, request.Object, request.Updates);
        }
        accessControlManager->ValidatePermissions(std::move(objectPermissions));
    }

    TPrefetchPassResult RunCreateObjectsPrefetchPass(
        IUpdateContext* context,
        const TTransactionCallContext& transactionCallContext,
        const std::vector<TCreateObjectSubrequest>& subrequests,
        const std::vector<TKeyAttributeMatches>& matchedKeyAttributes)
    {
        TTraceContextGuard guard(
            CreateTraceContextFromCurrent("NYT::NOrm::TTransaction::RunCreateObjectsPrefetchPass"));

        const auto size = std::ssize(subrequests);
        TPrefetchPassResult result;
        result.Objects.reserve(size);
        for (int index = 0; index < size; ++index) {
            auto& request = subrequests[index];
            const auto& matchedAttributes = matchedKeyAttributes[index];

            result.Objects.push_back(Session_->CreateObject(
                request.Type,
                matchedAttributes.Key,
                matchedAttributes.ParentKey,
                /*allowExisting*/ request.UpdateIfExisting.has_value()));
            auto* object = result.Objects.back();

            if (object->GetState() != EObjectState::Creating) {
                YT_VERIFY(request.UpdateIfExisting);
                DoUpdateObject(
                    object,
                    request.UpdateIfExisting->Requests,
                    request.UpdateIfExisting->Prerequisites,
                    context,
                    transactionCallContext,
                    request.UpdateIfExisting->Consumer);
                continue;
            }

            result.CreatedObjects.emplace_back(object, ParseCreationAttributes(
                request, matchedAttributes.Key));

            const auto& matches = result.CreatedObjects.back().second.Matches;
            for (const auto& match : matches) {
                PreloadAttributeUpdate(Owner_, object, match);
            }
        }

        return result;
    }

    void RunCreateObjectsApplyAttributesPass(
        IUpdateContext* context,
        const TTransactionCallContext& transactionCallContext,
        const std::vector<std::pair<TObject*, TCreationAttributeMatches>>& createdObjects,
        const std::vector<TKeyAttributeMatches>& /*matchedKeyAttributes*/)
    {
        for (auto& [object, matchedAttributes] : createdObjects) {
            const auto& matches = matchedAttributes.Matches;

            THashSet<const TAttributeSchema*> updatedAttributes;
            for (const auto& match : matches) {
                const auto* schema = match.Schema;
                const auto* setRequest = std::get_if<TSetUpdateRequest>(&match.Request);
                YT_VERIFY(setRequest);
                ValidateUpdateRequest(*setRequest);
                context->AddPreparer([=, this, request = std::move(*setRequest)] (IUpdateContext* /*context*/) {
                    try {
                        schema->RunValueSetter(
                            Owner_,
                            object,
                            request.Path,
                            request.Value,
                            request.Recursive,
                            /*sharedWrite*/ std::nullopt,
                            /*aggregateMode*/ EAggregateMode::Unspecified,
                            transactionCallContext);
                    } catch (const std::exception& ex) {
                        THROW_ERROR_EXCEPTION("Error setting attribute %Qv of %v",
                            schema->FormatPathEtc() + GetUpdateRequestPath(match.Request),
                            object->GetDisplayName(/*keyOnly*/ true))
                            << ex;
                    }
                });

                const TAttributeSchema* current = schema;
                while (current && updatedAttributes.insert(current).second) {
                    current = current->GetParent();
                }
            }

            for (const auto* schema : matchedAttributes.PendingInitializerAttributes) {
                schema->RunInitializer(Owner_, object);
            }

            OnAttributesUpdated(object, updatedAttributes, context);

            context->AddFinalizer([object, this] (IUpdateContext*) {
                object->GetTypeHandler()->FinishObjectCreation(Owner_, object);
            });
        }
    }

    void PerformExistenceChecksAndSchedulePermissionsValidation(
        IUpdateContext* context,
        const TAccessControlManagerPtr& accessControlManager,
        const TObjectManagerPtr& objectManager,
        std::vector<TCreateObjectSubrequest>& subrequests,
        const std::vector<TKeyAttributeMatches>& matchedKeyAttributes)
    {
        TTraceContextGuard guard(CreateTraceContextFromCurrent(
            "NYT::NOrm::TTransaction::PerformExistenceChecksAndSchedulePermissionsValidation"));

        const auto size = std::ssize(subrequests);
        std::vector<NAccessControl::TObjectPermission> permissionsToValidate;
        permissionsToValidate.reserve(size);
        TCompactSet<TObjectTypeValue, 8> nonexistentTypes;
        for (int index = 0; index < size; ++index) {
            auto& request = subrequests[index];
            const auto& matchedAttributes = matchedKeyAttributes[index];
            auto* typeHandler = objectManager->GetTypeHandlerOrThrow(request.Type);
            auto parentType = typeHandler->GetParentType();
            if (parentType == TObjectTypeValues::Null) {
                THROW_ERROR_EXCEPTION_IF(matchedAttributes.ParentKey,
                    "Objects of type %Qlv do not require explicit parent",
                    NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(request.Type));
            } else {
                THROW_ERROR_EXCEPTION_UNLESS(matchedAttributes.ParentKey,
                    "Objects of type %Qlv require explicit parent of type %Qlv",
                    NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(request.Type),
                    NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(parentType));
            }

            if (Session_->ExistenceCheckSuccessful(request.Type, matchedAttributes.Key)) {
                auto* object = Session_->GetObject(request.Type, matchedAttributes.Key, matchedAttributes.ParentKey);
                THROW_ERROR_EXCEPTION_UNLESS(request.UpdateIfExisting,
                    "%v already exists",
                    object->GetDisplayName(/*keyOnly*/ true));
                PopulateUpdateRequestsFromAttributes(
                    request.Attributes,
                    request.UpdateIfExisting->Requests);
                FillObjectPermissions(permissionsToValidate, object, request.UpdateIfExisting->Requests);
            } else {
                nonexistentTypes.insert(request.Type);
                if (parentType != TObjectTypeValues::Null) {
                    permissionsToValidate.push_back(NAccessControl::TObjectPermission{
                        .Object = Session_->GetObject(parentType, matchedAttributes.ParentKey),
                        .AttributePath = TYPath(AddChildAccessControlPath),
                        .Permission = TAccessControlPermissionValues::Write,
                    });
                }
            }
        }
        if (!nonexistentTypes.empty()) {
            TCompactFlatMap<TObjectTypeValue, TObject*, 8> schemas;
            for (auto type : nonexistentTypes) {
                if (schemas.contains(type)) {
                    continue;
                }

                schemas.insert({
                    type,
                    GetObject(
                        TObjectTypeValues::Schema,
                        TObjectKey(NClient::NObjects::GetGlobalObjectTypeRegistry()
                            ->GetTypeNameByValueOrCrash(type)))
                });
            }

            for (auto& [typeValue, object] : schemas) {
                permissionsToValidate.push_back(NAccessControl::TObjectPermission{
                    .Object = object,
                    .Permission = TAccessControlPermissionValues::Create,
                });
            }
        }

        if (!permissionsToValidate.empty()) {
            auto checkPermissions = [accessControlManager, permissions = std::move(permissionsToValidate)]
                (IUpdateContext* /*context*/)
            {
                accessControlManager->ValidatePermissions(std::move(permissions));
            };

            context->AddPreparer(std::move(checkPermissions));
        }
    }

    std::vector<TKeyAttributeMatches> MatchAllKeyAttributes(
        const TObjectManagerPtr& objectManager,
        const std::vector<TCreateObjectSubrequest>& subrequests)
    {
        TTraceContextGuard guard(CreateTraceContextFromCurrent("NYT::NOrm::TTransaction::MatchAllKeyAttributes"));

        const auto size = std::ssize(subrequests);
        std::vector<TKeyAttributeMatches> matchedKeyAttributes;
        matchedKeyAttributes.reserve(size);
        for (const auto& request : subrequests) {
            matchedKeyAttributes.push_back(
                MatchKeyAttributes(objectManager->GetTypeHandlerOrThrow(request.Type),
                    request.Attributes->FindChild("meta"),
                    Owner_,
                    /*autogenerateKey*/ !request.UpdateIfExisting));
        }
        return matchedKeyAttributes;
    }
};

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(
    NMaster::IBootstrap* bootstrap,
    TTransactionConfigsSnapshot configsSnapshot,
    TTransactionId id,
    TTimestamp startTimestamp,
    TYTTransactionOrClientDescriptor ytTransactionOrClient,
    std::string identityUserTag,
    TTransactionOptions options)
    : Impl_(New<TImpl>(
        this,
        bootstrap,
        std::move(configsSnapshot),
        id,
        startTimestamp,
        std::move(ytTransactionOrClient),
        std::move(identityUserTag),
        std::move(options)))
{ }

TTransaction::~TTransaction()
{ }

ETransactionState TTransaction::GetState() const
{
    return Impl_->GetState();
}

TTransactionId TTransaction::GetId() const
{
    return Impl_->GetId();
}

TTimestamp TTransaction::GetStartTimestamp() const
{
    return Impl_->GetStartTimestamp();
}

TInstant TTransaction::GetStartTime() const
{
    return Impl_->GetStartTime();
}

TInstant TTransaction::GetCommitStartTime() const
{
    return Impl_->GetCommitStartTime();
}

TInstant TTransaction::GetCommitFinishTime() const
{
    return Impl_->GetCommitFinishTime();
}

THistoryTime TTransaction::GetHistoryEventTime(const THistoryTableBase* history) const
{
    return Impl_->GetHistoryEventTime(history);
}

void TTransaction::EnsureReadWrite() const
{
    Impl_->EnsureReadWrite();
}

const TMutatingTransactionOptions& TTransaction::GetMutatingTransactionOptions() const
{
    return Impl_->GetMutatingTransactionOptions();
}

ISession* TTransaction::GetSession()
{
    return Impl_->GetSession();
}

NMaster::IBootstrap* TTransaction::GetBootstrap()
{
    return Impl_->GetBootstrap();
}

TTransactionManagerConfigPtr TTransaction::GetConfig() const
{
    return Impl_->GetConfig();
}

std::unique_ptr<IUpdateContext> TTransaction::CreateUpdateContext()
{
    return Impl_->CreateUpdateContext();
}

std::unique_ptr<ILoadContext> TTransaction::CreateLoadContext(
    TTestingStorageOptions testingStorageOptions)
{
    return Impl_->CreateLoadContext(std::move(testingStorageOptions));
}

std::unique_ptr<IStoreContext> TTransaction::CreateStoreContext()
{
    return Impl_->CreateStoreContext();
}

std::vector<TObject*> TTransaction::CreateObjects(
    std::vector<TCreateObjectSubrequest> subrequests,
    const TTransactionCallContext& transactionCallContext)
{
    TTraceContextGuard guard(CreateTraceContextFromCurrent("NYT::NOrm::TTransaction::CreateObjects"));

    return Impl_->CreateObjects(std::move(subrequests), transactionCallContext);
}

TObject* TTransaction::CreateObjectInternal(
    TObjectTypeValue type,
    TObjectKey key,
    TObjectKey parentKey,
    bool allowExisting)
{
    return Impl_->CreateObjectInternal(type, std::move(key), std::move(parentKey), allowExisting);
}

void TTransaction::RemoveObject(TObject* object)
{
    TTraceContextGuard guard(CreateTraceContextFromCurrent("NYT::NOrm::TTransaction::RemoveObject"));

    return Impl_->RemoveObjects({object});
}

void TTransaction::RemoveObjects(std::vector<TObject*> objects)
{
    TTraceContextGuard guard(CreateTraceContextFromCurrent("NYT::NOrm::TTransaction::RemoveObjects"));

    return Impl_->RemoveObjects(std::move(objects));
}

void TTransaction::RemoveObjectInternal(TObject* object)
{
    return Impl_->RemoveObjectInternal(object);
}

void TTransaction::UpdateObject(
    TObject* object,
    std::vector<TUpdateRequest> requests,
    std::vector<TAttributeTimestampPrerequisite> prerequisites,
    IUpdateContext* context,
    const TTransactionCallContext& transactionCallContext)
{
    TTraceContextGuard guard(CreateTraceContextFromCurrent("NYT::NOrm::TTransaction::UpdateObject"));

    return Impl_->UpdateObjects({
        TObjectUpdateRequest{
            .Object = object,
            .Updates = std::move(requests),
            .Prerequisites = std::move(prerequisites),
        }},
        context,
        transactionCallContext);
}

void TTransaction::UpdateObjects(
    std::vector<TObjectUpdateRequest> objectUpdates,
    IUpdateContext* context,
    const TTransactionCallContext& transactionCallContext)
{
    TTraceContextGuard guard(CreateTraceContextFromCurrent("NYT::NOrm::TTransaction::UpdateObjects"));

    return Impl_->UpdateObjects(
        std::move(objectUpdates),
        context,
        transactionCallContext);
}

TObject* TTransaction::GetObject(TObjectTypeValue type, TObjectKey key, TObjectKey parentKey)
{
    return Impl_->GetObject(type, std::move(key), std::move(parentKey));
}

IUnversionedRowsetPtr TTransaction::SelectFields(
    TObjectTypeValue type,
    const std::vector<const TDBField*>& fields,
    std::source_location location)
{
    return Impl_->SelectFields(type, fields, location);
}

void TTransaction::RunPrecommitActions(TTransactionContext context)
{
    Impl_->RunPrecommitActions(context);
}

TFuture<TTransactionCommitResult> TTransaction::Commit(TTransactionContext context) noexcept
{
    return Impl_->Commit(std::move(context));
}

TFuture<void> TTransaction::Abort() noexcept
{
    return Impl_->Abort();
}

TAsyncSemaphoreGuard TTransaction::AcquireLock()
{
    return Impl_->AcquireLock();
}

////////////////////////////////////////////////////////////////////////////////

TPerformanceStatistics TTransaction::FlushPerformanceStatistics()
{
    return Impl_->FlushPerformanceStatistics();
}

const TPerformanceStatistics& TTransaction::GetTotalPerformanceStatistics()
{
    return Impl_->GetTotalPerformanceStatistics();
}

void TTransaction::EnableAccessControlPreload()
{
    Impl_->EnableAccessControlPreload();
}

bool TTransaction::AccessControlPreloadEnabled() const
{
    return Impl_->AccessControlPreloadEnabled();
}

void TTransaction::UpdateRequestTimeout(std::optional<TDuration> timeout)
{
    Impl_->UpdateRequestTimeout(std::move(timeout));
}

std::optional<TDuration> TTransaction::GetRequestTimeoutRemaining()
{
    return Impl_->GetRequestTimeoutRemaining();
}

void TTransaction::SetReadPhaseLimit(i64 limit)
{
    Impl_->SetReadPhaseLimit(limit);
}

void TTransaction::AllowFullScan(bool allowFullScan)
{
    Impl_->AllowFullScan(allowFullScan);
}

bool TTransaction::FullScanAllowed() const
{
    return Impl_->FullScanAllowed();
}

void TTransaction::AllowRemovalWithNonEmptyReferences(bool allow)
{
    Impl_->AllowRemovalWithNonEmptyReferences(allow);
}

std::optional<bool> TTransaction::RemovalWithNonEmptyReferencesAllowed() const
{
    return Impl_->RemovalWithNonEmptyReferencesAllowed();
}

void TTransaction::AddAfterCommitAction(TAction action)
{
    Impl_->AddAfterCommitAction(std::move(action));
}

void TTransaction::AddAfterAbortAction(TAction action)
{
    Impl_->AddAfterAbortAction(std::move(action));
}

////////////////////////////////////////////////////////////////////////////////

bool TTransaction::ScheduleCommitAction(TCommitActionType type, TObject* object)
{
    return Impl_->ScheduleCommitAction(type, object);
}

THashSet<TObject*> TTransaction::ExtractCommitActions(TCommitActionType type)
{
    return Impl_->ExtractCommitActions(type);
}

////////////////////////////////////////////////////////////////////////////////

const std::optional<TString>& TTransaction::GetExecutionPoolTag() const
{
    return Impl_->GetExecutionPoolTag();
}

////////////////////////////////////////////////////////////////////////////////

void TTransaction::PrepareCommit()
{
    Impl_->PrepareCommit();
}

void TTransaction::PostCommit()
{
    Impl_->PostCommit();
}

////////////////////////////////////////////////////////////////////////////////

void TTransaction::PerformAttributeMigrations()
{
    Impl_->PerformAttributeMigrations();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
