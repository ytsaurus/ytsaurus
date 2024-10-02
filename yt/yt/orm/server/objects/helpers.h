#pragma once

#include "public.h"

#include <yt/yt/orm/client/objects/type.h>

#include <yt/yt/orm/library/attributes/unwrapping_consumer.h>
#include <yt/yt/orm/library/attributes/yson_builder.h>

#include <yt/yt/library/query/base/ast.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/yson/forwarding_consumer.h>

#include <source_location>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

void PrefetchAttributeTimestamp(
    const TResolveAttributeResult& resolveResult,
    const TObject* object);

TTimestamp FetchAttributeTimestamp(
    const TResolveAttributeResult& resolveResult,
    const TObject* object);

////////////////////////////////////////////////////////////////////////////////

class TValuePresentConsumer
    : public NYson::IYsonConsumer
{
public:
    explicit TValuePresentConsumer(IYsonConsumer* underlying);

    bool IsValuePresent();

    void OnStringScalar(TStringBuf value) override;
    void OnInt64Scalar(i64 value) override;
    void OnUint64Scalar(ui64 scalar) override;
    void OnDoubleScalar(double value) override;
    void OnBooleanScalar(bool value) override;
    void OnEntity() override;
    void OnBeginList() override;
    void OnListItem() override;
    void OnEndList() override;
    void OnBeginMap() override;
    void OnKeyedItem(TStringBuf key) override;
    void OnEndMap() override;
    void OnBeginAttributes() override;
    void OnEndAttributes() override;
    void OnRaw(TStringBuf yson, NYson::EYsonType type) override;

private:
    NYson::IYsonConsumer* const Underlying_;
    bool ValuePresent_ = false;
};

using NAttributes::TUnwrappingConsumer;
using NAttributes::TYsonStringBuilder;

////////////////////////////////////////////////////////////////////////////////

struct TFilterResolveResult
{
    NQueryClient::NAst::TExpressionPtr FilterExpression = nullptr;
    std::vector<TString> FullPaths;
    std::vector<TResolveAttributeResult> ResolvedAttributes;
};

TStringBuilder BuildSelectQuery(
    const NYPath::TYPath& tablePath,
    const TDBFields& fieldsToSelect,
    const TObjectKey& key,
    const TDBFields& keyFields);

TFilterResolveResult ResolveFilterExpression(
    NQueryClient::NAst::TExpressionPtr filter,
    IQueryContext* context,
    bool isComputed = false);

std::pair<NQueryClient::NAst::TExpressionPtr, NQueryClient::NAst::TExpressionPtr> BuildFilterExpressions(
    IQueryContext* context,
    const TObjectFilter& filter,
    bool computedFilterEnabled = false);

NQueryClient::NAst::TExpressionList RewriteExpressions(
    IQueryContext* context,
    const std::vector<TString>& expressions);

////////////////////////////////////////////////////////////////////////////////

NYTree::INodePtr RunConsumedValueGetter(
    const TScalarAttributeSchema* schema,
    TTransaction* transaction,
    const TObject* object,
    const NYPath::TYPath& path = {});

////////////////////////////////////////////////////////////////////////////////

NTableClient::EValueType ProtobufToTableValueType(
    NYson::TProtobufScalarElement::TType type,
    NYson::EEnumYsonStorageType enumStorageType);

EAttributeType ValueTypeToAttributeType(NTableClient::EValueType valueType);
NTableClient::EValueType AttributeTypeToValueType(EAttributeType attributeType);

TString GetYsonExtractFunction(NTableClient::EValueType type);

////////////////////////////////////////////////////////////////////////////////

TFuture<NTabletClient::TTableMountInfoPtr> GetTableMountInfo(
    NMaster::TYTConnectorPtr ytConnector,
    const TDBTable* table);

////////////////////////////////////////////////////////////////////////////////

TTimestamp GetBarrierTimestamp(const std::vector<NYT::NApi::TTabletInfo>& tabletInfos);

////////////////////////////////////////////////////////////////////////////////

TStringBuf TryGetTableNameFromQuery(TStringBuf query);

////////////////////////////////////////////////////////////////////////////////

template <class TTypedObject, typename TTypedValue>
TValueGetter MakeValueGetter(
    TTypedValue (TTypedObject::* getter)(std::source_location) const);

template <class TTypedObject, typename TTypedValue>
TValueGetter MakeValueGetter(
    TTypedValue (TTypedObject::* getter)() const);

////////////////////////////////////////////////////////////////////////////////

template <typename TContainer>
auto EmplaceBatch(TContainer& container, ui32 count);

template <typename TAttributeList, typename TField>
TAttributeList* AccessAttributeList(TField& field);

////////////////////////////////////////////////////////////////////////////////

// Call object->ScheduleMetaResponseLoad() first.
void GetConsumingObjectIdentityMeta(
    NYson::IYsonConsumer* consumer,
    TTransaction* transaction,
    const TObject* object);
NYson::TYsonString GetObjectIdentityMeta(
    TTransaction* transaction,
    const TObject* object);

////////////////////////////////////////////////////////////////////////////////

TEventTypeValue GetEventTypeValueByNameOrThrow(const TEventTypeName& eventTypeName);
TEventTypeName GetEventTypeNameByValueOrThrow(TEventTypeValue eventTypeValue);

////////////////////////////////////////////////////////////////////////////////

bool HasCompositeOrPolymorphicKey(IObjectTypeHandler* typeHandler);

////////////////////////////////////////////////////////////////////////////////

std::set<NYPath::TYPath>::iterator FindPathParent(const std::set<NYPath::TYPath>& parents, const NYPath::TYPath& path);

////////////////////////////////////////////////////////////////////////////////

NQueryClient::NAst::TExpressionPtr CastBoolToString(
    NQueryClient::NAst::TExpressionPtr expression,
    IQueryContext* context);
NQueryClient::NAst::TExpressionPtr CastUint64ToString(
    NQueryClient::NAst::TExpressionPtr expression,
    IQueryContext* context);
NQueryClient::NAst::TExpressionPtr CastNumericToString(
    NQueryClient::NAst::TExpressionPtr expression,
    IQueryContext* context);

////////////////////////////////////////////////////////////////////////////////

using TComputedFieldsDetector = std::function<bool(NQueryClient::NAst::TReferenceExpressionPtr)>;

TComputedFieldsDetector MakeComputedFieldsDetector(NObjects::IQueryContext* context);

////////////////////////////////////////////////////////////////////////////////

std::vector<TResolveAttributeResult> CollectAttributes(
    IQueryContext* context,
    const TString& query);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
