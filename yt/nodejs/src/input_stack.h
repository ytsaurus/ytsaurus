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
    explicit TNodeJSInputStack(TInputStreamWrap* base);
    virtual ~TNodeJSInputStack() throw();

    void AddCompression(ECompression compression);

    ui64 GetBytes() const;

protected:
    virtual size_t DoRead(void* data, size_t length) override;

private:
    TInputStreamWrap* GetBaseStream();
    const TInputStreamWrap* GetBaseStream() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
