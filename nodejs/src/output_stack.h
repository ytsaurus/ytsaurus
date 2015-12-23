#pragma once

#include "output_stream.h"
#include "stream_stack.h"

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

class TNodeJSOutputStack
    : public TOutputStream
    , public TGrowingOutputStreamStack
{
public:
    TNodeJSOutputStack(TOutputStreamWrap* base);
    virtual ~TNodeJSOutputStack() throw();

    TOutputStreamWrap* GetBaseStream();

    void AddCompression(ECompression compression);

    bool HasAnyData();

protected:
    virtual void DoWrite(const void* buffer, size_t length) override;
    virtual void DoWriteV(const TPart* parts, size_t count) override;
    virtual void DoFlush() override;
    virtual void DoFinish() override;

private:
    bool HasAnyData_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
