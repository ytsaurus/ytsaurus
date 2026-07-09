#pragma once

#ifndef VERSIONED_VALUE_H_
    #error "Direct inclusion of this file is not allowed, include versioned_value.h"
    // For the sake of sane code completion.
    #include "versioned_value.h"
#endif

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class T>
constexpr bool IsIntrusivePtr = false;

template <class T>
constexpr bool IsIntrusivePtr<TIntrusivePtr<T>> = true;

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <typename TValue>
TVersion TVersionedValue<TValue>::GetVersion() const
{
    return Version_;
}

template <typename TValue>
const TValue& TVersionedValue<TValue>::GetValue() const
{
    return Value_;
}

template <typename TValue>
TInstant TVersionedValue<TValue>::GetLastUpdate() const
{
    return LastUpdate_;
}

template <typename TValue>
void TVersionedValue<TValue>::SetValue(TValue newValue)
{
    auto tmp = New<TVersionedValue>();
    tmp->Value_ = std::move(newValue);
    tmp->Version_ = Version_;
    tmp->LastUpdate_ = LastUpdate_;
    if (AreNodesEqual(ConvertTo<NYTree::INodePtr>(*this), ConvertTo<NYTree::INodePtr>(tmp))) {
        return;
    }
    Value_ = std::move(tmp->Value_);
    LastUpdate_ = TInstant::Now();
    BumpVersion();
}

template <typename TValue>
void TVersionedValue<TValue>::BumpVersion()
{
    Bump(&Version_);
}

template <typename TValue>
void TVersionedValue<TValue>::Register(TRegistrar registrar)
{
    registrar.Parameter("version", &TThis::Version_)
        .Default();
    if constexpr (NDetail::IsIntrusivePtr<TValue>) {
        registrar.Parameter("value", &TThis::Value_)
            .DefaultNew();
    } else {
        registrar.Parameter("value", &TThis::Value_)
            .Default();
    }
    registrar.Parameter("last_update", &TThis::LastUpdate_)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
