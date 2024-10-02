#ifndef TYPE_HANDLER_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include type_handler_detail.h"
// For the sake of sane code completion.
#include "type_handler_detail.h"
#endif

#include "attribute_schema_builder.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <typename TDescriptor>
TScalarAttributeSchema* TObjectTypeHandlerBase::MakeScalarAttributeSchema(
    const TString& name,
    const TDescriptor& descriptor)
{
    auto schema = MakeScalarAttributeSchema(name);
    return TScalarAttributeSchemaBuilder(schema).SetAttribute(descriptor);
}

template <typename TDescriptor>
TScalarAttributeSchema* TObjectTypeHandlerBase::MakeEtcAttributeSchema(
    const TString& name,
    const TDescriptor& descriptor)
{
    // NB! Order matters here. Current implementation expects SetEtc() before SetAttribute().
    auto schema = MakeScalarAttributeSchema(name)->SetEtc();
    return TScalarAttributeSchemaBuilder(schema).SetAttribute(descriptor);
}

template <typename TDescriptor>
TScalarAttributeSchema* TObjectTypeHandlerBase::MakeEtcAttributeSchema(
    const TDescriptor& descriptor)
{
    return MakeEtcAttributeSchema(/*name*/ {}, descriptor);
}

template <class TTypedObject, class TTypedValue>
TScalarAttributeSchema* TObjectTypeHandlerBase::MakeProtobufAttributeSchema(
    const TString& name,
    const TScalarAttributeDescriptor<TTypedObject, TString>& descriptor)
{
    auto* schema = MakeScalarAttributeSchema(name);
    return TScalarAttributeSchemaBuilder(schema).SetProtobufAttribute<
        TTypedObject, TTypedValue>(descriptor);
}

template <typename TDescriptor>
TScalarAttributeSchema* TObjectTypeHandlerBase::MakeAccessControlParentIdAttributeSchema(
    const TString& name,
    const TDescriptor& descriptor)
{
    const auto& keyFields = GetAccessControlParentKeyFields();
    AccessControlParentIdAttributeSchemas_.resize(keyFields.size());

    const TDBField* field = descriptor.Field;
    const auto index = FindIndex(keyFields, field);

    if (index == NPOS) {
        THROW_ERROR_EXCEPTION("Access control parent key field %Qv not found in %Qv",
            field->Name,
            SchemaId_);
    }

    if (AccessControlParentIdAttributeSchemas_[index] != nullptr) {
        THROW_ERROR_EXCEPTION("Access control parent key field %Qv attached to attribute schemas twice",
            field->Name);
    }

    auto* schema = MakeScalarAttributeSchema(name, descriptor);
    AccessControlParentIdAttributeSchemas_[index] = schema;
    return schema;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
