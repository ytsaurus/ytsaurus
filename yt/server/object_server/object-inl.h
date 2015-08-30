#ifndef OBJECT_INL_H_
#error "Direct inclusion of this file is not allowed, include object.h"
#endif

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

inline TObjectBase::TObjectBase(const TObjectId& id)
    : Id_(id)
{ }

inline TObjectBase::~TObjectBase()
{
    // To make debugging easier.
    RefCounter_ = DisposedRefCounter;
}

inline void TObjectBase::SetDestroyed()
{
    YASSERT(RefCounter_ == 0);
    RefCounter_ = DestroyedRefCounter;
}

inline const TObjectId& TObjectBase::GetId() const
{
    return Id_;
}

inline int TObjectBase::RefObject()
{
    YASSERT(RefCounter_ >= 0);
    return ++RefCounter_;
}

inline int TObjectBase::UnrefObject(int count)
{
    YASSERT(RefCounter_ >= count);
    return RefCounter_ -= count;
}

inline int TObjectBase::WeakRefObject()
{
    YCHECK(IsAlive());
    YASSERT(WeakRefCounter_ >= 0);
    return ++WeakRefCounter_;
}

inline int TObjectBase::WeakUnrefObject()
{
    YASSERT(WeakRefCounter_ > 0);
    return --WeakRefCounter_;
}

inline int TObjectBase::ImportRefObject()
{
    return ++ImportRefCounter_;
}

inline int TObjectBase::ImportUnrefObject()
{
    YASSERT(ImportRefCounter_ > 0);
    return --ImportRefCounter_;
}

inline void TObjectBase::ResetWeakRefCounter()
{
    WeakRefCounter_ = 0;
}

inline int TObjectBase::GetObjectRefCounter() const
{
    return RefCounter_;
}

inline int TObjectBase::GetObjectWeakRefCounter() const
{
    return WeakRefCounter_;
}

inline int TObjectBase::GetImportRefCounter() const
{
    return ImportRefCounter_;
}

inline bool TObjectBase::IsAlive() const
{
    return RefCounter_ > 0;
}

inline bool TObjectBase::IsDestroyed() const
{
    return RefCounter_ == DestroyedRefCounter;
}

inline bool TObjectBase::IsLocked() const
{
    return WeakRefCounter_ > 0;
}

////////////////////////////////////////////////////////////////////////////////

inline TNonversionedObjectBase::TNonversionedObjectBase(const TObjectId& id)
    : TObjectBase(id)
{ }

////////////////////////////////////////////////////////////////////////////////

inline TObjectId GetObjectId(const TObjectBase* object)
{
    return object ? object->GetId() : NullObjectId;
}

inline bool IsObjectAlive(const TObjectBase* object)
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

template <class TKey, class TValue, class THash>
std::vector<TValue*> GetValuesSortedByKey(const NHydra::TReadOnlyEntityMap<TKey, TValue, THash>& entities)
{
    std::vector<TValue*> values;
    for (const auto& pair : entities) {
        auto* object = pair.second;
        if (IsObjectAlive(object)) {
            values.push_back(object);
        }
    }

    std::sort(
        values.begin(),
        values.end(),
        [] (TValue* lhs, TValue* rhs) { return lhs->GetId() < rhs->GetId(); });
    return values;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
