#pragma once

#include "public.h"

#include "attribute_schema.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <class TTypedValue>
concept CStringValidatableValue = std::is_same_v<TTypedValue, TString> ||
    std::is_same_v<TTypedValue, std::vector<TString>> ||
    (NMpl::IsSpecialization<TTypedValue, THashMap> &&
        std::is_same_v<typename TTypedValue::key_type, TString> &&
        std::is_convertible_v<typename TTypedValue::mapped_type*, google::protobuf::Message*>);

template <class TDescriptor, class TObject, class TTypedValue>
concept CScalarOrAggregatedAttributeDescriptor =
    std::is_same_v<TScalarAttributeDescriptor<TObject, TTypedValue>, TDescriptor> ||
    std::is_same_v<TScalarAggregatedAttributeDescriptor<TObject, TTypedValue>, TDescriptor>;

////////////////////////////////////////////////////////////////////////////////

class TScalarAttributeSchemaBuilder
{
public:
    TScalarAttributeSchemaBuilder(TScalarAttributeSchema* schema)
        : Schema_(schema)
    { }

    template <std::derived_from<TObject> TTypedObject, class TTypedValue>
    TScalarAttributeSchema* SetAttribute(const TScalarAttributeDescriptor<TTypedObject, TTypedValue>& descriptor);
    template <std::derived_from<TObject> TTypedObject, class TTypedValue>
    TScalarAttributeSchema* SetAttribute(const TScalarAggregatedAttributeDescriptor<TTypedObject, TTypedValue>& descriptor);

    template <class TOne, class TMany>
    TScalarAttributeSchema* SetAttribute(const TManyToOneAttributeDescriptor<TMany, TOne>& descriptor);
    template <class TOne, class TMany>
    TScalarAttributeSchema* SetAttribute(const TOneToManyAttributeDescriptor<TOne, TMany>& descriptor);
    template <class TThis, class TThat>
    TScalarAttributeSchema* SetAttribute(const TOneToOneAttributeDescriptor<TThis, TThat>& descriptor);
    template <class TOwner, class TForeign>
    TScalarAttributeSchema* SetAttribute(const TManyToManyInlineAttributeDescriptor<TOwner, TForeign>& descriptor);
    template <class TOwner, class TForeign>
    TScalarAttributeSchema* SetAttribute(const TManyToManyTabularAttributeDescriptor<TOwner, TForeign>& descriptor);

    template <class TOwner, class TForeign>
    TScalarAttributeSchema* SetAttribute(const TOneTransitiveAttributeDescriptor<TOwner, TForeign>& descriptor);

    template <class TMany, class TOne>
    TScalarAttributeSchema* SetAttribute(const TManyToOneViewAttributeDescriptor<TMany, TOne>& descriptor);
    template <class TOwner, class TForeign>
    TScalarAttributeSchema* SetAttribute(const TManyToManyInlineViewAttributeDescriptor<TOwner, TForeign>& descriptor);

    template <class TOwner, class TForeign>
    TScalarAttributeSchema* SetAttribute(
        const TSingleReferenceDescriptor<TOwner, TForeign>& descriptor);
    template <class TOwner, class TForeign>
    TScalarAttributeSchema* SetAttribute(
        const TMultiReferenceDescriptor<TOwner, TForeign>& descriptor);

    template <class TOwner, class TForeign>
    TScalarAttributeSchema* SetAttribute(
        const TSingleViewDescriptor<TOwner, TForeign>& descriptor);
    template <class TOwner, class TForeign>
    TScalarAttributeSchema* SetAttribute(
        const TMultiViewDescriptor<TOwner, TForeign>& descriptor);

    template <class TTypedObject, class TTypedValue>
    TScalarAttributeSchema* SetProtobufAttribute(
        const TScalarAttributeDescriptor<TTypedObject, TString>& descriptor);

    TScalarAttributeSchema* SetAnnotationsAttribute();

private:
    TScalarAttributeSchema* const Schema_;

    template <class TTypedObject, class TTypedValue>
    void InitValueSetter(const TScalarAttributeDescriptor<TTypedObject, TTypedValue>& descriptor);
    template <class TTypedObject, class TTypedValue>
    void InitValueGetter(const TScalarAttributeDescriptor<TTypedObject, TTypedValue>& descriptor);
    template <class TTypedValue>
    void InitDefaultValueGetter();
    template <class TTypedObject, class TTypedValue, class TDescriptor>
    void InitStoreScheduledGetter(const TDescriptor& descriptor);
    template <class TTypedObject, class TTypedValue>
    void InitChangedGetter(const TScalarAttributeDescriptor<TTypedObject, TTypedValue>& descriptor);
    template <class TTypedObject, class TTypedValue>
    void InitFilteredChangedGetter(const TScalarAttributeDescriptor<TTypedObject, TTypedValue>& descriptor);
    template <
        class TTypedObject,
        class TTypedValue,
        CScalarOrAggregatedAttributeDescriptor<TTypedObject, TTypedValue> TDescriptor
    >
    void InitTimestampGetter(const TDescriptor& descriptor);
    void InitTimestampExpressionBuilder(const TScalarAttributeDescriptorBase& descriptor);
    template <class TTypedObject, class TTypedValue>
    void InitInitializer(const TScalarAttributeDescriptor<TTypedObject, TTypedValue>& descriptor);
    template <class TTypedObject, class TTypedValue>
    void InitRemover(const TScalarAttributeDescriptor<TTypedObject, TTypedValue>& descriptor);
    template <
        class TTypedObject,
        class TTypedValue,
        CScalarOrAggregatedAttributeDescriptor<TTypedObject, TTypedValue> TDescriptor
    >
    void InitLocker(const TDescriptor& descriptor);
    template <class TTypedObject, class TTypedValue>
    void InitPreloader(const TScalarAttributeDescriptor<TTypedObject, TTypedValue>& descriptor);
    template <class TTypedObject, class TTypedValue>
    void InitUpdatePreloader(const TScalarAttributeDescriptor<TTypedObject, TTypedValue>& descriptor);

    template <class TDescriptor>
    void InitListContainsExpressionBuilder(const TDescriptor& descriptor);

    template <class TOwner, class TForeign, template<class, class> class TReferenceDescriptor>
    void SetReferenceCommon(
        const TReferenceDescriptor<TOwner, TForeign>& descriptor);
    template <class TOwner, class TForeign>
    void SetViewCommon();

    template <class TTypedObject, class TTypedValue, class TDescriptor>
    void AddPostValidatorIfNecessary(const TDescriptor& /*descriptor*/);
    template <
        class TTypedObject,
        CStringValidatableValue TTypedValue,
        CScalarOrAggregatedAttributeDescriptor<TTypedObject, TTypedValue> TDescriptor
    >
    void AddPostValidatorIfNecessary(const TDescriptor& descriptor);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define ATTRIBUTE_SCHEMA_BUILDER_INL_H_
#include "attribute_schema_builder-inl.h"
#undef ATTRIBUTE_SCHEMA_BUILDER_INL_H_
