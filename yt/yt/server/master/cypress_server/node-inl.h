#pragma once
#ifndef NODE_INL_H_
#error "Direct inclusion of this file is not allowed, include node.h"
// For the sake of sane code completion.
#include "node.h"
#endif

#include <yt/core/misc/variant.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

inline TCypressNodeDynamicData* TCypressNode::GetDynamicData() const
{
    return GetTypedDynamicData<TCypressNodeDynamicData>();
}

inline int TCypressNode::GetAccessStatisticsUpdateIndex() const
{
    return GetDynamicData()->AccessStatisticsUpdateIndex;
}

inline void TCypressNode::SetAccessStatisticsUpdateIndex(int value)
{
    GetDynamicData()->AccessStatisticsUpdateIndex = value;
}

inline int TCypressNode::GetTouchNodesIndex() const
{
    return GetDynamicData()->TouchNodesIndex;
}

inline void TCypressNode::SetTouchNodesIndex(int value)
{
    GetDynamicData()->TouchNodesIndex = value;
}

inline std::optional<TCypressNodeExpirationMap::iterator> TCypressNode::GetExpirationTimeIterator() const
{
    return GetDynamicData()->ExpirationTimeIterator;
}

inline void TCypressNode::SetExpirationTimeIterator(std::optional<TCypressNodeExpirationMap::iterator> value)
{
    GetDynamicData()->ExpirationTimeIterator = value;
}

inline std::optional<TCypressNodeExpirationMap::iterator> TCypressNode::GetExpirationTimeoutIterator() const
{
    return GetDynamicData()->ExpirationTimeoutIterator;
}

inline void TCypressNode::SetExpirationTimeoutIterator(std::optional<TCypressNodeExpirationMap::iterator> value)
{
    GetDynamicData()->ExpirationTimeoutIterator = value;
}

////////////////////////////////////////////////////////////////////////////////

inline bool TCypressNodeRefComparer::Compare(const TCypressNode* lhs, const TCypressNode* rhs)
{
    return lhs->GetVersionedId() < rhs->GetVersionedId();
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TVersionedBuiltinAttribute<T>::Persist(const NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, BoxedValue_);
}

template <class T>
void TVersionedBuiltinAttribute<T>::Persist(const NCypressServer::TCopyPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, BoxedValue_);
}

template <class T>
std::optional<T> TVersionedBuiltinAttribute<T>::Set(T value)
{
    if constexpr (std::is_pointer_v<T>) {
        // nullptrs are not allowed; Remove() should be used instead.
        YT_VERIFY(value);
    }

    auto result = ToOptional();
    BoxedValue_ = std::move(value);
    return result;
}

template <class T>
void TVersionedBuiltinAttribute<T>::SetOrReset(T value)
{
    if constexpr (std::is_pointer_v<T>) {
        if (value) {
            Set(std::move(value));
        } else {
            Reset();
        }
    } else {
        Set(std::move(value));
    }
}

template <class T>
std::optional<T> TVersionedBuiltinAttribute<T>::Reset()
{
    auto result = ToOptional();
    if constexpr (std::is_pointer_v<T>) {
        BoxedValue_ = std::nullopt;
    } else {
        BoxedValue_ = TNullVersionedBuiltinAttribute{};
    }
    return result;
}

template <class T>
std::optional<T> TVersionedBuiltinAttribute<T>::Remove()
{
    auto result = ToOptional();
    if constexpr (std::is_pointer_v<T>) {
        BoxedValue_ = nullptr;
    } else {
        BoxedValue_ = TTombstonedVersionedBuiltinAttribute{};
    }
    return result;
}

template <class T>
bool TVersionedBuiltinAttribute<T>::IsNull() const
{
    if constexpr (std::is_pointer_v<T>) {
        return !BoxedValue_.has_value();
    } else {
        return std::holds_alternative<TNullVersionedBuiltinAttribute>(BoxedValue_);
    }
}

template <class T>
bool TVersionedBuiltinAttribute<T>::IsTombstoned() const
{
    if constexpr (std::is_pointer_v<T>) {
        return BoxedValue_ && *BoxedValue_ == nullptr;
    } else {
        return std::holds_alternative<TTombstonedVersionedBuiltinAttribute>(BoxedValue_);
    }
}

template <class T>
bool TVersionedBuiltinAttribute<T>::IsSet() const
{
    if constexpr (std::is_pointer_v<T>) {
        return BoxedValue_ && *BoxedValue_ != nullptr;
    } else {
        return std::holds_alternative<T>(BoxedValue_);
    }
}

template <class T>
T TVersionedBuiltinAttribute<T>::Unbox() const
{
    if constexpr (std::is_pointer_v<T>) {
        auto* result = *BoxedValue_;
        YT_VERIFY(result);
        return result;
    } else {
        auto* result = std::get_if<T>(&BoxedValue_);
        YT_VERIFY(result);
        return *result;
    }
}

template <class T>
std::optional<T> TVersionedBuiltinAttribute<T>::ToOptional() const
{
    return IsSet() ? std::make_optional(Unbox()) : std::nullopt;
}

template <class T>
template <class TOwner>
/*static*/ T TVersionedBuiltinAttribute<T>::Get(
    TVersionedBuiltinAttribute<T> TOwner::*member,
    const TOwner* node)
{
    auto result = TryGet(member, node);
    YT_VERIFY(result);
    return *result;
}

template <class T>
template <class TOwner>
/*static*/ T TVersionedBuiltinAttribute<T>::Get(
    const TVersionedBuiltinAttribute<T>* (TOwner::*memberGetter)() const,
    const TOwner* node)
{
    auto result = TryGet(memberGetter, node);
    YT_VERIFY(result);
    return *result;
}

template <class T>
template <class TOwner>
/*static*/ std::optional<T> TVersionedBuiltinAttribute<T>::TryGet(
    TVersionedBuiltinAttribute<T> TOwner::*member,
    const TOwner* node)
{
    for (auto* currentNode = node; currentNode; currentNode = currentNode->GetOriginator()->template As<TOwner>()) {
        const auto& attribute = currentNode->*member;

        if (attribute.IsNull()) {
            continue;
        }

        if (attribute.IsTombstoned()) {
            return std::nullopt;
        }

        return attribute.Unbox();
    }

    return std::nullopt;
}

template <class T>
template <class TOwner>
/*static*/ std::optional<T> TVersionedBuiltinAttribute<T>::TryGet(
    const TVersionedBuiltinAttribute<T>* (TOwner::*memberGetter)() const,
    const TOwner* node)
{
    for (auto* currentNode = node; currentNode; currentNode = currentNode->GetOriginator()->template As<TOwner>()) {
        auto* attribute = (currentNode->*memberGetter)();

        if (!attribute) {
            continue;
        }

        if (attribute->IsNull()) {
            continue;
        }

        if (attribute->IsTombstoned()) {
            return std::nullopt;
        }

        return attribute->Unbox();
    }

    return std::nullopt;
}

template <class T>
void TVersionedBuiltinAttribute<T>::Merge(const TVersionedBuiltinAttribute& from, bool isTrunk)
{
    if (from.IsTombstoned()) {
        if (isTrunk) {
            Reset();
        } else {
            Remove();
        }
    } else if (!from.IsNull()) {
        Set(from.Unbox());
    } // NB: null attributes are ignored.
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
