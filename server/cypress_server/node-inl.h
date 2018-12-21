#pragma once
#ifndef NODE_INL_H_
#error "Direct inclusion of this file is not allowed, include node.h"
// For the sake of sane code completion.
#include "node.h"
#endif

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

inline TCypressNodeDynamicData* TCypressNodeBase::GetDynamicData() const
{
    return GetTypedDynamicData<TCypressNodeDynamicData>();
}

inline int TCypressNodeBase::GetAccessStatisticsUpdateIndex() const
{
    return GetDynamicData()->AccessStatisticsUpdateIndex;
}

inline void TCypressNodeBase::SetAccessStatisticsUpdateIndex(int value)
{
    GetDynamicData()->AccessStatisticsUpdateIndex = value;
}

inline std::optional<TCypressNodeExpirationMap::iterator> TCypressNodeBase::GetExpirationIterator() const
{
    return GetDynamicData()->ExpirationIterator;
}

inline void TCypressNodeBase::SetExpirationIterator(std::optional<TCypressNodeExpirationMap::iterator> value)
{
    GetDynamicData()->ExpirationIterator = value;
}

////////////////////////////////////////////////////////////////////////////////

inline bool TCypressNodeRefComparer::Compare(const TCypressNodeBase* lhs, const TCypressNodeBase* rhs)
{
    return lhs->GetVersionedId() < rhs->GetVersionedId();
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TVersionedBuiltinAttribute<T>::TNull::Persist(NCellMaster::TPersistenceContext& context)
{ }

template <class T>
void TVersionedBuiltinAttribute<T>::TTombstone::Persist(NCellMaster::TPersistenceContext& context)
{ }

template <class T>
template <class TOwner>
const T& TVersionedBuiltinAttribute<T>::Get(
    TVersionedBuiltinAttribute<T> TOwner::*member,
    const TOwner* node) const
{
    auto* currentNode = node;
    while (true) {
        Y_ASSERT(currentNode);
        const auto& attribute = currentNode->*member;
        if (const auto* value = attribute.BoxedValue_.template TryAs<T>()) {
            return *value;
        }
        currentNode = currentNode->GetOriginator()->template As<TOwner>();
    }
}

template <class T>
void TVersionedBuiltinAttribute<T>::Set(T value)
{
    BoxedValue_ = std::move(value);
}

template <class T>
void TVersionedBuiltinAttribute<T>::Reset()
{
    BoxedValue_ = TNull();
}

template <class T>
void TVersionedBuiltinAttribute<T>::Remove()
{
    BoxedValue_ = TTombstone();
}

template <class T>
template <class TOwner>
void TVersionedBuiltinAttribute<T>::Merge(
    TVersionedBuiltinAttribute<T> TOwner::*member,
    TOwner* originatingNode,
    const TOwner* branchedNode)
{
    const auto& branchedAttribute = branchedNode->*member;
    if (branchedAttribute.BoxedValue_.template Is<TTombstone>()) {
        if (originatingNode->IsTrunk()) {
            BoxedValue_ = TNull();
        } else {
            BoxedValue_ = TTombstone();
        }
    } else if (const auto* value = branchedAttribute.BoxedValue_.template TryAs<T>()) {
        BoxedValue_ = *value;
    }
}

template <class T>
void TVersionedBuiltinAttribute<T>::Persist(NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, BoxedValue_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
