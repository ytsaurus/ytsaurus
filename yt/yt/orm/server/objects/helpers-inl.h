#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

#include "object.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template<typename T>
struct TTypeToValueType
{ };

template<typename T>
    requires TEnumTraits<T>::IsEnum
struct TTypeToValueType<T>
{
    const static NTableClient::EValueType ValueType = NTableClient::EValueType::Any;
};

#define MAP_TYPE_TO_VALUE_TYPE(T, value_type) \
template<> \
struct TTypeToValueType<T> \
{ \
    const static NTableClient::EValueType ValueType = NTableClient::value_type; \
}

MAP_TYPE_TO_VALUE_TYPE(double, EValueType::Double);
MAP_TYPE_TO_VALUE_TYPE(float, EValueType::Double);
MAP_TYPE_TO_VALUE_TYPE(i64, EValueType::Int64);
MAP_TYPE_TO_VALUE_TYPE(ui64, EValueType::Uint64);
MAP_TYPE_TO_VALUE_TYPE(i32, EValueType::Int64);
MAP_TYPE_TO_VALUE_TYPE(ui32, EValueType::Uint64);
MAP_TYPE_TO_VALUE_TYPE(TString, EValueType::String);
MAP_TYPE_TO_VALUE_TYPE(bool, EValueType::Boolean);
MAP_TYPE_TO_VALUE_TYPE(NYTree::IMapNodePtr, EValueType::Any);
MAP_TYPE_TO_VALUE_TYPE(NYT::TGuid, EValueType::String);
MAP_TYPE_TO_VALUE_TYPE(TInstant, EValueType::Uint64);

#undef MAP_TYPE_TO_VALUE_TYPE

////////////////////////////////////////////////////////////////////////////////

template <class TTypedObject, typename TTypedValue>
TValueGetter MakeValueGetter(
    TTypedValue (TTypedObject::* getter)(std::source_location) const)
{
    return [getter] (
        TTransaction* /*transaction*/,
        const TObject* object,
        NYson::IYsonConsumer* consumer,
        const NYPath::TYPath& /*path*/)
    {
        auto* typedObject = object->template As<TTypedObject>();
        // TODO(dgolear): Pass source_location to ValueGetter.
        NYTree::ConvertToProducer((typedObject->*getter)(std::source_location::current())).Run(consumer);
    };
}

template <class TTypedObject, typename TTypedValue>
TValueGetter MakeValueGetter(
    TTypedValue (TTypedObject::* getter)() const)
{
    return [getter] (
        TTransaction* /*transaction*/,
        const TObject* object,
        NYson::IYsonConsumer* consumer,
        const NYPath::TYPath& /*path*/)
    {
        auto* typedObject = object->template As<TTypedObject>();
        NYTree::ConvertToProducer((typedObject->*getter)()).Run(consumer);
    };
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
struct TEmplaceBatchTraits
{ };

template <typename T>
   requires std::is_default_constructible_v<T>
struct TEmplaceBatchTraits<std::vector<T>>
{
    static auto Impl(std::vector<T>& container, int count, int initialSize)
    {
        YT_VERIFY(initialSize == std::ssize(container));
        container.reserve(initialSize + count);
        for (int i = 0; i < count; ++i) {
            container.emplace_back();
        }
        return std::pair(container.begin() + initialSize, container.end());
    }
};

template <typename T>
struct TEmplaceBatchTraits<google::protobuf::RepeatedPtrField<T>>
{
    static auto Impl(google::protobuf::RepeatedPtrField<T>& container, int count, int initialSize)
    {
        YT_VERIFY(initialSize == std::ssize(container));
        container.Reserve(initialSize + count);
        for (int i = 0; i < count; ++i) {
            container.Add();
        }
        return std::pair(container.begin() + initialSize, container.end());
    }
};

template <typename T>
struct TEmplaceBatchTraits<std::span<T>>
{
    static auto Impl(std::span<T>& span, int count, int initialSize)
    {
        YT_VERIFY(count + initialSize <= std::ssize(span));
        return std::pair(span.begin() + initialSize, span.begin() + initialSize + count);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TAttributeList, typename TField>
struct TAccessAttributeListTraits
{ };

template <typename TAttributeList>
struct TAccessAttributeListTraits<TAttributeList, TAttributeList>
{
    static TAttributeList* Impl(TAttributeList& field)
    {
        return &field;
    }
};

template <typename TAttributeList, typename TField>
    requires requires (TField message)
    {
        { message.mutable_result() } -> std::same_as<TAttributeList*>;
    }
struct TAccessAttributeListTraits<TAttributeList, TField>
{
    static TAttributeList* Impl(TField& field)
    {
        return field.mutable_result();
    }
};

template <typename TAttributeList, typename TField>
    requires requires (TField message)
    {
        { message.mutable_results() } -> std::same_as<TAttributeList*>;
    }
struct TAccessAttributeListTraits<TAttributeList, TField>
{
    static TAttributeList* Impl(TField& field)
    {
        return field.mutable_results();
    }
};

template <typename TAttributeList, typename TField>
    requires requires (TField message)
    {
        { message.mutable_method_results() } -> std::same_as<TAttributeList*>;
    }
struct TAccessAttributeListTraits<TAttributeList, TField>
{
    static TAttributeList* Impl(TField& field)
    {
        return field.mutable_method_results();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <typename TContainer>
auto EmplaceBatch(TContainer& container, int count, int alreadyAllocatedCount)
{
    return NDetail::TEmplaceBatchTraits<TContainer>::Impl(container, count, alreadyAllocatedCount);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TAttributeList, typename TField>
TAttributeList* AccessAttributeList(TField& field)
{
    return NDetail::TAccessAttributeListTraits<TAttributeList, TField>::Impl(field);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
