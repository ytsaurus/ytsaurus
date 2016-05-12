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
    explicit TNodeJSOutputStack(TOutputStreamWrap* base);
    virtual ~TNodeJSOutputStack() throw();

    void AddCompression(ECompression compression);

    ui64 GetBytes() const;

protected:
    virtual void DoWrite(const void* buffer, size_t length) override;
    virtual void DoWriteV(const TPart* parts, size_t count) override;
    virtual void DoFlush() override;
    virtual void DoFinish() override;

private:
    TOutputStreamWrap* GetBaseStream();
    const TOutputStreamWrap* GetBaseStream() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
