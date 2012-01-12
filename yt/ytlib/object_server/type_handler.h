#pragma once

#include "id.h"

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

struct IObjectTypeHandler
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IObjectTypeHandler> TPtr;

    virtual EObjectType GetType() = 0;

    virtual bool Exists(const TObjectId& id) = 0;

    virtual i32 RefObject(const TObjectId& id) = 0;
    virtual i32 UnrefObject(const TObjectId& id) = 0;
    virtual i32 GetObjectRefCounter(const TObjectId& id) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

