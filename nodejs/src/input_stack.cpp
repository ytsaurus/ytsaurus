#include "input_stack.h"


#include <library/streams/lz/lz.h>
#include <library/streams/lzop/lzop.h>

#include <util/stream/zlib.h>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

TNodeJSInputStack::TNodeJSInputStack(
    TInputStreamWrap* base,
    IInvokerPtr invoker)
    : TGrowingStreamStack(base)
    , Invoker_(std::move(invoker))
{
    THREAD_AFFINITY_IS_V8();
    Y_ASSERT(Bottom() == base);
    GetBaseStream()->AsyncRef();
}

TNodeJSInputStack::~TNodeJSInputStack() throw()
{
    GetBaseStream()->AsyncUnref();
}

TInputStreamWrap* TNodeJSInputStack::GetBaseStream()
{
    return static_cast<TInputStreamWrap*>(Bottom());
}

const TInputStreamWrap* TNodeJSInputStack::GetBaseStream() const
{
    return static_cast<const TInputStreamWrap*>(Bottom());
}

void TNodeJSInputStack::AddCompression(ECompression compression)
{
    AddCompressionToStack(*this, compression);
}

ui64 TNodeJSInputStack::GetBytes() const
{
    return GetBaseStream()->GetBytesEnqueued();
}

TFuture<size_t> TNodeJSInputStack::Read(const TSharedMutableRef& buffer)
{
    return
        BIND(&TNodeJSInputStack::SyncRead, MakeStrong(this), buffer)
        .AsyncVia(Invoker_)
        .Run();
}

size_t TNodeJSInputStack::SyncRead(const TSharedMutableRef& buffer)
{
    // Using Load() here to amortize context switching costs.
    return Top()->Load(buffer.Begin(), buffer.Size());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
