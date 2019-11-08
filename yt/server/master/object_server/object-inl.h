#pragma once
#include "object.h"

#ifndef OBJECT_INL_H_
#error "Direct inclusion of this file is not allowed, include object.h"
// For the sake of sane code completion.
#include "object.h"
#endif

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

inline TObject::TObject(TObjectId id)
    : Id_(id)
{
    // This is reset to false in TCypressNode ctor for non-trunk nodes.
    Flags_.Trunk = true;
}

inline TObject::~TObject()
{
    // To make debugging easier.
    Flags_.Disposed = true;
}

inline TObjectDynamicData* TObject::GetDynamicData() const
{
    return GetTypedDynamicData<TObjectDynamicData>();
}

inline void TObject::SetDestroyed()
{
    YT_ASSERT(RefCounter_ == 0);
    Flags_.Destroyed = true;
}

inline void TObject::SetForeign()
{
    Flags_.Foreign = true;
}

inline TObjectId TObject::GetId() const
{
    return Id_;
}

inline int TObject::RefObject()
{
    YT_ASSERT(RefCounter_ >= 0);
    return ++RefCounter_;
}

inline int TObject::UnrefObject(int count)
{
    YT_ASSERT(RefCounter_ >= count);
    return RefCounter_ -= count;
}

inline int TObject::EphemeralRefObject(TEpoch epoch)
{
    YT_VERIFY(IsAlive());
    YT_ASSERT(EphemeralRefCounter_ >= 0);

    if (epoch != EphemeralLockEpoch_) {
        EphemeralRefCounter_ = 0;
        EphemeralLockEpoch_ = epoch;
    }
    return ++EphemeralRefCounter_;
}

inline int TObject::EphemeralUnrefObject(TEpoch epoch)
{
    YT_ASSERT(EphemeralRefCounter_ > 0);
    YT_ASSERT(EphemeralLockEpoch_ == epoch);
    return --EphemeralRefCounter_;
}

inline int TObject::WeakRefObject()
{
    YT_VERIFY(IsAlive());
    YT_ASSERT(WeakRefCounter_ >= 0);

    return ++WeakRefCounter_;
}

inline int TObject::WeakUnrefObject()
{
    YT_ASSERT(WeakRefCounter_ > 0);
    return --WeakRefCounter_;
}

inline int TObject::ImportRefObject()
{
    return ++ImportRefCounter_;
}

inline int TObject::ImportUnrefObject()
{
    YT_ASSERT(ImportRefCounter_ > 0);
    return --ImportRefCounter_;
}

inline int TObject::GetObjectRefCounter() const
{
    return RefCounter_;
}

inline int TObject::GetObjectEphemeralRefCounter(TEpoch epoch) const
{
    return EphemeralLockEpoch_== epoch ? EphemeralRefCounter_ : 0;
}

inline int TObject::GetObjectWeakRefCounter() const
{
    return WeakRefCounter_;
}

inline int TObject::GetImportRefCounter() const
{
    return ImportRefCounter_;
}

inline EObjectLifeStage TObject::GetLifeStage() const
{
    return LifeStage_;
}

inline void TObject::SetLifeStage(EObjectLifeStage lifeStage)
{
    LifeStage_ = lifeStage;
}

inline bool TObject::IsBeingCreated() const
{
    return LifeStage_ == EObjectLifeStage::CreationStarted ||
        LifeStage_ == EObjectLifeStage::CreationPreCommitted;
}

inline bool TObject::IsAlive() const
{
    return RefCounter_ > 0;
}

inline bool TObject::IsBeingRemoved() const
{
    return LifeStage_ == EObjectLifeStage::RemovalStarted ||
        LifeStage_ == EObjectLifeStage::RemovalPreCommitted ||
        LifeStage_ == EObjectLifeStage::RemovalCommitted;
}

inline bool TObject::IsDestroyed() const
{
    return Flags_.Destroyed;
}

inline bool TObject::IsTrunk() const
{
    return Flags_.Trunk;
}

inline bool TObject::IsForeign() const
{
    return Flags_.Foreign;
}

inline bool TObject::IsNative() const
{
    return !IsForeign();
}

template <class TDerived>
TDerived* TObject::As()
{
    return static_cast<TDerived*>(this);
}

template <class TDerived>
const TDerived* TObject::As() const
{
    return static_cast<const TDerived*>(this);
}

////////////////////////////////////////////////////////////////////////////////

inline TNonversionedObjectBase::TNonversionedObjectBase(TObjectId id)
    : TObject(id)
{ }

////////////////////////////////////////////////////////////////////////////////

inline bool TObjectRefComparer::Compare(const TObject* lhs, const TObject* rhs)
{
    return lhs->GetId() < rhs->GetId();
}

inline TObjectId GetObjectId(const TObject* object)
{
    return object ? object->GetId() : NullObjectId;
}

inline bool IsObjectAlive(const TObject* object)
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
    values.reserve(entities.size());

    for (const auto& pair : entities) {
        auto* object = pair.second;
        if (IsObjectAlive(object)) {
            values.push_back(object);
        }
    }
    std::sort(values.begin(), values.end(), TObjectRefComparer::Compare);
    return values;
}

template <class TValue>
std::vector<TValue*> GetValuesSortedByKey(const THashSet<TValue*>& entities)
{
    std::vector<TValue*> values;
    values.reserve(entities.size());

    for (auto* object : entities) {
        if (IsObjectAlive(object)) {
            values.push_back(object);
        }
    }
    std::sort(values.begin(), values.end(), TObjectRefComparer::Compare);
    return values;
}

template <class TObject, class TValue>
std::vector<typename THashMap<TObject*, TValue>::iterator> GetIteratorsSortedByKey(THashMap<TObject*, TValue>& entities)
{
    std::vector<typename THashMap<TObject*, TValue>::iterator> iterators;
    iterators.reserve(entities.size());

    for (auto it = entities.begin(); it != entities.end(); ++it) {
        if (IsObjectAlive(it->first)) {
            iterators.push_back(it);
        }
    }
    std::sort(iterators.begin(), iterators.end(), [] (auto lhs, auto rhs) {
        return TObjectRefComparer::Compare(lhs->first, rhs->first);
    });
    return iterators;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
