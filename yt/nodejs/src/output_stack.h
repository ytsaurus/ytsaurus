#pragma once

#include "output_stream.h"
#include "stream_stack.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TNodeJSOutputStack
    : public TOutputStream
    , public TGrowingStreamStack<TOutputStream, 3>
{
public:
    TNodeJSOutputStack(TNodeJSOutputStream* base);
    virtual ~TNodeJSOutputStack() throw();

    TNodeJSOutputStream* GetBaseStream();

    void AddCompression(ECompression compression);

protected:
    virtual void DoWrite(const void* buffer, size_t length) override;
    virtual void DoFlush() override;
    virtual void DoFinish() override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
