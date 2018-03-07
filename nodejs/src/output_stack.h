#pragma once

#include "output_stream.h"
#include "stream_stack.h"

#include <yt/core/concurrency/async_stream.h>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

class TNodeJSOutputStack
    : private TGrowingOutputStreamStack
    , public NConcurrency::IAsyncOutputStream
{
public:
    TNodeJSOutputStack(
        TOutputStreamWrap* base,
        IInvokerPtr invoker);
    virtual ~TNodeJSOutputStack() throw();

    void AddCompression(ECompression compression);

    ui64 GetBytes() const;

    virtual TFuture<void> Write(const TSharedRef& buffer) override;

    virtual TFuture<void> Close() override;

private:
    TOutputStreamWrap* GetBaseStream();
    const TOutputStreamWrap* GetBaseStream() const;

    void SyncWrite(const TSharedRef& buffer);
    void SyncClose();

    IInvokerPtr Invoker_;
};

DECLARE_REFCOUNTED_TYPE(TNodeJSOutputStack)
DEFINE_REFCOUNTED_TYPE(TNodeJSOutputStack)

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
