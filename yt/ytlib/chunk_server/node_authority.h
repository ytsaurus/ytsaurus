#pragma once

#include "public.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

//! Provides an interface for checking node authorization.
struct INodeAuthority
    : public virtual TRefCounted
{
    //! Returns true iff a node with the given address is authorized to register.
    virtual bool IsAuthorized(const Stroka& address) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
