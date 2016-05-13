#include "input_stack.h"

#include <yt/core/utilex/lzop.h>

#include <library/streams/lz/lz.h>

#include <util/stream/zlib.h>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

TNodeJSInputStack::TNodeJSInputStack(TInputStreamWrap* base)
    : TGrowingStreamStack(base)
{
    THREAD_AFFINITY_IS_V8();
    YASSERT(Bottom() == base);
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

size_t TNodeJSInputStack::DoRead(void* data, size_t length)
{
    return Top()->Read(data, length);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
