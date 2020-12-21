#pragma once

#include "poller.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! A base class for implementing IPollable.
class TPollableBase
    : public IPollable
{
public:
    virtual void SetCookie(TCookiePtr cookie) override;
    virtual void* GetCookie() const override;

private:
    TCookiePtr Cookie_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
