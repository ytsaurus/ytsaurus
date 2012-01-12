#pragma once

#include "id.h"

#include <yt/ytlib/misc/property.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TObjectBase
{
public:
    TObjectBase();

    //! Increments the object's reference counter.
    /*!
     *  \returns the incremented counter.
     */
    i32 RefObject();

    //! Decrements the object's reference counter.
    /*!
     *  \note
     *  Objects do not self-destruct, it's callers responsibility to check
     *  if the counter reaches zero.
     *  
     *  \returns the decremented counter.
     */
    i32 UnrefObject();

    //! Returns the current reference counter.
    i32 GetObjectRefCounter() const;

protected:
    TObjectBase(const TObjectBase& other);

    i32 RefCounter;

};

////////////////////////////////////////////////////////////////////////////////

class TObjectWithIdBase
    : public TObjectBase
{
    DEFINE_BYVAL_RO_PROPERTY(TObjectId, Id);

public:
    TObjectWithIdBase();
    TObjectWithIdBase(const TObjectId& id);
    TObjectWithIdBase(const TObjectWithIdBase& other);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

