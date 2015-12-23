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

inline int TObjectBase::UnrefObject()
{
    YASSERT(RefCounter_ > 0);
    return --RefCounter_;
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
