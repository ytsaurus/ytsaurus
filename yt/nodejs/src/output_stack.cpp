#include "output_stack.h"

#include <yt/core/utilex/lzop.h>

#include <library/streams/lz/lz.h>

#include <util/stream/zlib.h>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

TNodeJSOutputStack::TNodeJSOutputStack(
    TOutputStreamWrap* base,
    IInvokerPtr invoker)
    : TGrowingStreamStack(base)
    , Invoker_(std::move(invoker))
{
    THREAD_AFFINITY_IS_V8();
    Y_ASSERT(Bottom() == base);
    GetBaseStream()->AsyncRef();
}

TNodeJSOutputStack::~TNodeJSOutputStack() throw()
{
    GetBaseStream()->AsyncUnref();
}

TOutputStreamWrap* TNodeJSOutputStack::GetBaseStream()
{
    return static_cast<TOutputStreamWrap*>(Bottom());
}

const TOutputStreamWrap* TNodeJSOutputStack::GetBaseStream() const
{
    return static_cast<const TOutputStreamWrap*>(Bottom());
}

void TNodeJSOutputStack::AddCompression(ECompression compression)
{
    AddCompressionToStack(*this, compression);
}

ui64 TNodeJSOutputStack::GetBytes() const
{
    return GetBaseStream()->GetBytesEnqueued();
}

TFuture<void> TNodeJSOutputStack::Write(const TSharedRef& buffer)
{
    return
        BIND(&TNodeJSOutputStack::SyncWrite, MakeStrong(this), buffer)
        .AsyncVia(Invoker_)
        .Run();
}

TFuture<void> TNodeJSOutputStack::Close()
{
    return
        BIND(&TNodeJSOutputStack::SyncClose, MakeStrong(this))
        .AsyncVia(Invoker_)
        .Run();
}

void TNodeJSOutputStack::SyncWrite(const TSharedRef& buffer)
{
    Top()->Write(buffer.Begin(), buffer.Size());
}

void TNodeJSOutputStack::SyncClose()
{
    GetBaseStream()->MarkAsFinishing();

    for (auto* current : *this) {
        current->Finish();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
