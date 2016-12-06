#pragma once
#ifndef NODE_INL_H_
#error "Direct inclusion of this file is not allowed, include node.h"
#endif

namespace NYT {
namespace NCypressServer {

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

inline TNullable<TCypressNodeExpirationMap::iterator> TCypressNodeBase::GetExpirationIterator() const
{
    return GetDynamicData()->ExpirationIterator;
}

inline void TCypressNodeBase::SetExpirationIterator(TNullable<TCypressNodeExpirationMap::iterator> value)
{
    GetDynamicData()->ExpirationIterator = value;
}

////////////////////////////////////////////////////////////////////////////////

inline bool TCypressNodeRefComparer::Compare(const TCypressNodeBase* lhs, const TCypressNodeBase* rhs)
{
    return lhs->GetVersionedId() < rhs->GetVersionedId();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
