#pragma once

#include "public.h"

namespace NYT {
namespace NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

//! Provides an interface for checking node authorization.
struct INodeAuthority
    : public virtual TRefCounted
{
    //! Returns |true| if a node with the given address is authorized to register.
    // TODO(babenko): use descriptor instead
    virtual bool IsAuthorized(const Stroka& address) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
