#pragma once

#include <util/stream/input.h>

class yexception;

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TProxyInput
    : public TInputStream
{
public:
    virtual bool OnStreamError(const yexception& e) = 0;
    virtual void OnRowFetched() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
