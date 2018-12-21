#pragma once
#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

#include "attribute_consumer.h"
#include "serialize.h"
#include "convert.h"

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T IAttributeDictionary::Get(const TString& key) const
{
    auto yson = GetYson(key);
    try {
        return ConvertTo<T>(yson);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing attribute %Qv",
            key)
            << ex;
    }
}

template <class T>
T IAttributeDictionary::GetAndRemove(const TString& key)
{
    auto result = Get<T>(key);
    Remove(key);
    return result;
}

template <class T>
T IAttributeDictionary::Get(const TString& key, const T& defaultValue) const
{
    return Find<T>(key).value_or(defaultValue);
}

template <class T>
T IAttributeDictionary::GetAndRemove(const TString& key, const T& defaultValue)
{
    auto result = Find<T>(key);
    if (result) {
        Remove(key);
        return *result;
    } else {
        return defaultValue;
    }
}

template <class T>
typename TOptionalTraits<T>::TOptional IAttributeDictionary::Find(const TString& key) const
{
    auto yson = FindYson(key);
    if (!yson) {
        return typename TOptionalTraits<T>::TOptional();
    }
    try {
        return ConvertTo<T>(yson);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing attribute %Qv",
            key)
            << ex;
    }
}

template <class T>
typename TOptionalTraits<T>::TOptional IAttributeDictionary::FindAndRemove(const TString& key)
{
    auto result = Find<T>(key);
    if (result) {
        Remove(key);
    }
    return result;
}

template <class T>
void IAttributeDictionary::Set(const TString& key, const T& value)
{
    auto yson = ConvertToYsonString(value, NYson::EYsonFormat::Binary);
    SetYson(key, yson);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TAttributeDictionaryRefSerializer::Save(TStreamSaveContext& context, const T& obj)
{
    using NYT::Save;
    if (obj) {
        Save(context, true);
        Save(context, *obj);
    } else {
        Save(context, false);
    }
}

template <class T>
void TAttributeDictionaryRefSerializer::Load(TStreamLoadContext& context, T& obj)
{
    using NYT::Load;
    if (Load<bool>(context)) {
        obj = CreateEphemeralAttributes();
        Load(context, *obj);
    } else {
        obj.reset();
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class T, class R>
IYPathServicePtr IYPathService::FromMethod(
    R (T::*method) () const,
    const TWeakPtr<T>& weak)
{
    auto producer = NYson::TYsonProducer(BIND([=] (NYson::IYsonConsumer* consumer) {
        auto strong = weak.Lock();
        if (strong) {
            Serialize((strong.Get()->*method)(), consumer);
        }
    }));

    return FromProducer(producer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
