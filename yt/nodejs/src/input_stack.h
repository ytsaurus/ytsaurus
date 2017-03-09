#pragma once

#include "input_stream.h"
#include "stream_stack.h"

#include <yt/core/concurrency/async_stream.h>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

class TNodeJSInputStack
    : private TGrowingInputStreamStack
    , public NConcurrency::IAsyncInputStream
{
public:
    TNodeJSInputStack(
        TInputStreamWrap* base,
        IInvokerPtr invoker);
    virtual ~TNodeJSInputStack() throw();

    void AddCompression(ECompression compression);

    ui64 GetBytes() const;

    virtual TFuture<size_t> Read(const TSharedMutableRef& buffer) override;

private:
    TInputStreamWrap* GetBaseStream();
    const TInputStreamWrap* GetBaseStream() const;

    size_t SyncRead(const TSharedMutableRef& buffer);

    IInvokerPtr Invoker_;
};

DECLARE_REFCOUNTED_TYPE(TNodeJSInputStack)
DEFINE_REFCOUNTED_TYPE(TNodeJSInputStack)

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
