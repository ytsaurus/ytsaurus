#pragma once

#include "public.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

//! Provides an interface for checking holder authorization.
struct IHolderAuthority
    : public virtual TRefCounted
{
    //! Returns true iff the holder with a given address is authorized to register.
    virtual bool IsHolderAuthorized(const Stroka& address) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
