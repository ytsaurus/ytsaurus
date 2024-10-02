#ifndef OBJECT_REFLECTION_INL_H_
#error "Direct inclusion of this file is not allowed, include object_reflection.h"
// For the sake of sane code completion.
#include "object_reflection.h"
#endif

#include "attribute_schema.h"

#include <yt/yt/orm/library/attributes/ytree.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <typename TValue>
TValue GetScalarValueOrThrow(
    TTransaction* transaction,
    TObject* object,
    const TString& path)
{
    const auto [schema, suffix] = ResolveAttributeValidated(object->GetTypeHandler(), path);
    const auto* scalarSchema = schema->TryAsScalar();
    THROW_ERROR_EXCEPTION_UNLESS(scalarSchema && scalarSchema->HasValueGetter(),
        "While locating field %v got attribute %v that is not a scalar in %v",
        path,
        schema->FormatPathEtc(),
        object->GetDisplayName());
    auto consumer = NYTree::CreateBuilderFromFactory(NYTree::GetEphemeralNodeFactory());
    consumer->BeginTree();
    scalarSchema->RunValueGetter(transaction, object, consumer.get(), suffix);
    return NYTree::ConvertTo<TValue>(consumer->EndTree());
}

template <typename TValue>
void SetScalarValueOrThrow(
    TTransaction* transaction,
    TObject* object,
    const TString& path,
    const TValue& value,
    bool recursive,
    std::optional<bool> sharedWrite,
    EAggregateMode aggregateMode,
    const TTransactionCallContext& transactionCallContext)
{
    const auto [schema, suffix] = ResolveAttributeValidated(object->GetTypeHandler(), path);
    const auto* scalarSchema = schema->TryAsScalar();
    THROW_ERROR_EXCEPTION_UNLESS(scalarSchema && scalarSchema->HasValueSetter(),
        "While locating field %v got attribute %v that is not a scalar in %v",
        path,
        schema->FormatPathEtc(),
        object->GetDisplayName());
    auto tree = NYTree::ConvertToNode(value);
    scalarSchema->RunValueSetter(
        transaction,
        object,
        suffix,
        tree,
        recursive,
        sharedWrite,
        aggregateMode,
        transactionCallContext);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
