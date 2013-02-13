#pragma once

#include "public.h"

#include <ytlib/misc/foreach.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

//! Provides a base for all objects in YT server.
class TObjectBase
    : private TNonCopyable
{
public:
    explicit TObjectBase(const TObjectId& id);

    //! Returns the object id.
    const TObjectId& GetId() const;

    //! Returns the object type.
    EObjectType GetType() const;

    //! Increments the object's reference counter.
    /*!
     *  \returns the incremented counter.
     */
    int RefObject();

    //! Decrements the object's reference counter.
    /*!
     *  \note
     *  Objects do not self-destruct, it's callers responsibility to check
     *  if the counter reaches zero.
     *
     *  \returns the decremented counter.
     */
    int UnrefObject();

    //! Returns the current reference counter.
    int GetObjectRefCounter() const;

    //! Returns True iff the reference counter is non-zero.
    bool IsAlive() const;

    //! Returns True iff the object is either non-versioned or versioned but does not belong to a transaction.
    bool IsTrunk() const;

protected:
    void Save(const NCellMaster::TSaveContext& context) const;
    void Load(const NCellMaster::TLoadContext& context);

    TObjectId Id;
    int RefCounter;

};

////////////////////////////////////////////////////////////////////////////////

template <class T>
std::vector<TObjectId> ToObjectIds(const std::vector<T*>& objects)
{
    std::vector<TObjectId> result;
    result.reserve(objects.size());
    FOREACH (auto* object, objects) {
        result.push_back(object->GetId());
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TUnversionedObjectBase
    : public TObjectBase
{
public:
    explicit TUnversionedObjectBase(const TObjectId& id);

};

////////////////////////////////////////////////////////////////////////////////

inline TObjectId GetObjectId(const TObjectBase* object)
{
    return object ? object->GetId() : NullObjectId;
}

template <class T>
struct TObjectIdTraits
{
    typedef decltype(GetObjectId(T())) TId;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

