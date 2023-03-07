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

inline std::optional<TCypressNodeExpirationMap::iterator> TCypressNode::GetExpirationIterator() const
{
    return GetDynamicData()->ExpirationIterator;
}

inline void TCypressNode::SetExpirationIterator(std::optional<TCypressNodeExpirationMap::iterator> value)
{
    GetDynamicData()->ExpirationIterator = value;
}

////////////////////////////////////////////////////////////////////////////////

inline bool TCypressNodeRefComparer::Compare(const TCypressNode* lhs, const TCypressNode* rhs)
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
T TVersionedBuiltinAttribute<T>::Get(
    TVersionedBuiltinAttribute<T> TOwner::*member,
    const TOwner* node) const
{
    auto result = TryGet(member, node);
    YT_VERIFY(result);
    return *result;
}

template <class T>
template <class TOwner>
std::optional<T> TVersionedBuiltinAttribute<T>::TryGet(
    TVersionedBuiltinAttribute<T> TOwner::*member,
    const TOwner* node) const
{
    for (auto* currentNode = node; currentNode; currentNode = currentNode->GetOriginator()->template As<TOwner>()) {
        const auto& attribute = currentNode->*member;

        auto [result, mustBreak] = Visit(attribute.BoxedValue_,
            [&] (TNull) {
                return std::pair<std::optional<T>, bool>(std::nullopt, false);
            },
            [&] (TTombstone) {
                return std::pair<std::optional<T>, bool>(std::nullopt, true);
            },
            [&] (const T& value) {
                return std::pair<std::optional<T>, bool>(value, true);
            });

        if (mustBreak) {
            return result;
        }
    }

    return std::nullopt;
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
    Visit(branchedAttribute.BoxedValue_,
        [&] (TTombstone) {
            if (originatingNode->IsTrunk()) {
                BoxedValue_ = TNull();
            } else {
                BoxedValue_ = TTombstone();
            }
        },
        [&] (const T& value) {
            BoxedValue_ = value;
        },
        [&] (TNull) {
            // ignore
        });
}

template <class T>
void TVersionedBuiltinAttribute<T>::Persist(NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, BoxedValue_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
