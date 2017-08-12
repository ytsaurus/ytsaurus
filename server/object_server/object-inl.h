#pragma once
#include "object.h"

#ifndef OBJECT_INL_H_
#error "Direct inclusion of this file is not allowed, include object.h"
#endif

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

inline IObjectBase::IObjectBase(const TObjectId& id)
    : Id_(id)
{
    // This is reset to false in TCypressNodeBase ctor for non-trunk nodes.
    Flags_.Trunk = true;
}

inline IObjectBase::~IObjectBase()
{
    // To make debugging easier.
    Flags_.Disposed = true;
}

inline TObjectDynamicData* IObjectBase::GetDynamicData() const
{
    return GetTypedDynamicData<TObjectDynamicData>();
}

inline void IObjectBase::SetDestroyed()
{
    Y_ASSERT(RefCounter_ == 0);
    Flags_.Destroyed = true;
}

inline void IObjectBase::SetForeign()
{
    Flags_.Foreign = true;
}

inline const TObjectId& IObjectBase::GetId() const
{
    return Id_;
}

inline int IObjectBase::RefObject()
{
    Y_ASSERT(RefCounter_ >= 0);
    return ++RefCounter_;
}

inline int IObjectBase::UnrefObject(int count)
{
    Y_ASSERT(RefCounter_ >= count);
    return RefCounter_ -= count;
}

inline int IObjectBase::WeakRefObject(TEpoch epoch)
{
    YCHECK(IsAlive());
    Y_ASSERT(WeakRefCounter_ >= 0);

    if (epoch != WeakLockEpoch_) {
        WeakRefCounter_ = 0;
        WeakLockEpoch_ = epoch;
    }
    return ++WeakRefCounter_;
}

inline int IObjectBase::WeakUnrefObject(TEpoch epoch)
{
    Y_ASSERT(WeakRefCounter_ > 0);
    Y_ASSERT(WeakLockEpoch_ == epoch);
    return --WeakRefCounter_;
}

inline int IObjectBase::ImportRefObject()
{
    return ++ImportRefCounter_;
}

inline int IObjectBase::ImportUnrefObject()
{
    Y_ASSERT(ImportRefCounter_ > 0);
    return --ImportRefCounter_;
}

inline int IObjectBase::GetObjectRefCounter() const
{
    return RefCounter_;
}

inline int IObjectBase::GetObjectWeakRefCounter(TEpoch epoch) const
{
    return WeakLockEpoch_== epoch ? WeakRefCounter_ : 0;
}

inline int IObjectBase::GetImportRefCounter() const
{
    return ImportRefCounter_;
}

inline bool IObjectBase::IsAlive() const
{
    return RefCounter_ > 0;
}

inline bool IObjectBase::IsDestroyed() const
{
    return Flags_.Destroyed;
}

inline bool IObjectBase::IsLocked() const
{
    return WeakRefCounter_ > 0;
}

inline bool IObjectBase::IsTrunk() const
{
    return Flags_.Trunk;
}

inline bool IObjectBase::IsForeign() const
{
    return Flags_.Foreign;
}

template <class TDerived>
TDerived* IObjectBase::As()
{
    return static_cast<TDerived*>(this);
}

template <class TDerived>
const TDerived* IObjectBase::As() const
{
    return static_cast<const TDerived*>(this);
}

////////////////////////////////////////////////////////////////////////////////

inline TNonversionedObjectBase::TNonversionedObjectBase(const TObjectId& id)
    : IObjectBase(id)
{ }

////////////////////////////////////////////////////////////////////////////////

inline bool TObjectRefComparer::Compare(const IObjectBase* lhs, const IObjectBase* rhs)
{
    return lhs->GetId() < rhs->GetId();
}

inline TObjectId GetObjectId(const IObjectBase* object)
{
    return object ? object->GetId() : NullObjectId;
}

inline bool IsObjectAlive(const IObjectBase* object)
{
    return object && object->IsAlive();
}

template <class T>
std::vector<TObjectId> ToObjectIds(const T& objects, size_t sizeLimit)
{
    std::vector<TObjectId> result;
    result.reserve(std::min(objects.size(), sizeLimit));
    for (auto* object : objects) {
        if (result.size() == sizeLimit)
            break;
        result.push_back(object->GetId());
    }
    return result;
}

template <class TValue>
std::vector<TValue*> GetValuesSortedByKey(const NHydra::TReadOnlyEntityMap<TValue>& entities)
{
    std::vector<TValue*> values;
    for (const auto& pair : entities) {
        auto* object = pair.second;
        if (IsObjectAlive(object)) {
            values.push_back(object);
        }
    }
    std::sort(values.begin(), values.end(), TObjectRefComparer::Compare);
    return values;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
