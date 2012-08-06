#pragma once

#include "input_stream.h"
#include "stream_stack.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TNodeJSInputStack
    : public TInputStream
    , public TGrowingStreamStack<TInputStream, 3>
{
public:
    TNodeJSInputStack(TNodeJSInputStream* base);
    virtual ~TNodeJSInputStack() throw();

    TNodeJSInputStream* GetBaseStream();

    void AddCompression(ECompression compression);

protected:
    virtual size_t DoRead(void* data, size_t length) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
