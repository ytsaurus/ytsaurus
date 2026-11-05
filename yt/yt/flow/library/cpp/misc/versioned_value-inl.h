#pragma once

#ifndef VERSIONED_VALUE_H_
    #error "Direct inclusion of this file is not allowed, include versioned_value.h"
    // For the sake of sane code completion.
    #include "versioned_value.h"
#endif

#include <yt/yt/client/transaction_client/helpers.h>

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
bool TVersionedValue<TValue>::TrySetValue(TValue newValue, const IVersionProviderPtr& versionProvider)
{
    auto tmp = New<TVersionedValue>();
    tmp->Value_ = std::move(newValue);
    tmp->Version_ = Version_;
    tmp->LastUpdate_ = LastUpdate_;
    if (AreNodesEqual(ConvertTo<NYTree::INodePtr>(*this), ConvertTo<NYTree::INodePtr>(tmp))) {
        return false;
    }
    tmp->Bump(versionProvider);
    Value_ = std::move(tmp->Value_);
    Version_ = tmp->Version_;
    LastUpdate_ = tmp->LastUpdate_;
    return true;
}

template <typename TValue>
void TVersionedValue<TValue>::Bump(const IVersionProviderPtr& versionProvider)
{
    auto version = versionProvider->GenerateVersion();
    YT_VERIFY(version > Version_);
    Version_ = version;
    // Stored for readable text YSON; derived from the version to keep both fields consistent.
    LastUpdate_ = NTransactionClient::TimestampToInstant(
        NTransactionClient::TTimestamp(version.Underlying()))
        .first;
}

template <typename TValue>
void TVersionedValue<TValue>::Register(TRegistrar registrar)
{
    registrar.Parameter("version", &TThis::Version_)
        .Default();
    if constexpr (NDetail::IsIntrusivePtr<TValue>) {
        // Only a yson-struct pointee can be default-constructed here; any other pointee
        // (e.g. INodePtr) defaults to null.
        if constexpr (std::derived_from<typename TValue::TUnderlying, NYT::NYTree::TYsonStructBase>) {
            registrar.Parameter("value", &TThis::Value_)
                .DefaultNew();
        } else {
            registrar.Parameter("value", &TThis::Value_)
                .Default();
        }
    } else {
        registrar.Parameter("value", &TThis::Value_)
            .Default();
    }
    registrar.Parameter("last_update", &TThis::LastUpdate_)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
