#pragma once

#include "raw_pipeline.h"  // ::NRoren::NPrivate::TTransformNode

#include <functional>

namespace NRoren::NPrivate
{

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_ROREN_TAG(TagName, TObjectType, TValueType) \
    void Set ## TagName(TObjectType& object, TValueType value); \
    const TValueType& Get ## TagName(const TObjectType& object); \
    const TValueType& Get ## TagName(const TObjectType& object, const TValueType& defaultValue); \
    std::optional<std::reference_wrapper<const TValueType>> Get ## TagName(const TObjectType& object, std::nothrow_t);

#define DEFINE_ROREN_TAG(TagName, TObjectType, TValueType, ObjectGetter) \
    const TTypeTag<TValueType> TagName ## Tag(#TagName); \
    \
    void Set ## TagName(TObjectType& object, TValueType value) \
    { \
        ::NRoren::NPrivate::SetAttribute(ObjectGetter, TagName ## Tag, std::move(value)); \
    } \
    \
    const TValueType& Get ## TagName(const TObjectType& object) \
    { \
        return ::NRoren::NPrivate::GetRequiredAttribute(ObjectGetter, TagName ## Tag); \
    } \
    \
    const TValueType& Get ## TagName(const TObjectType& object, const TValueType& defaultValue) \
    { \
        return ::NRoren::NPrivate::GetAttributeOrDefault(ObjectGetter, TagName ## Tag, defaultValue); \
    } \
    \
    std::optional<std::reference_wrapper<const TValueType>> Get ## TagName(const TObjectType& object, std::nothrow_t) \
    { \
        const auto* valuePointer = ::NRoren::NPrivate::GetAttribute(ObjectGetter, TagName ## Tag); \
        if (valuePointer) { \
            return std::cref(*valuePointer); \
        } \
        return {}; \
    }

////////////////////////////////////////////////////////////////////////////////

}  // namespace NRoren::NPrivate
