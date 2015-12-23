#pragma once

#include "input_stream.h"
#include "stream_stack.h"

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

class TNodeJSInputStack
    : public TInputStream
    , public TGrowingInputStreamStack
{
public:
    TNodeJSInputStack(TInputStreamWrap* base);
    virtual ~TNodeJSInputStack() throw();

    TInputStreamWrap* GetBaseStream();

    void AddCompression(ECompression compression);

protected:
    virtual size_t DoRead(void* data, size_t length) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
