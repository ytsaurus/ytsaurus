#include "input_stack.h"

#include <core/utilex/lzop.h>

#include <util/stream/zlib.h>

#include <library/streams/lz/lz.h>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

TNodeJSInputStack::TNodeJSInputStack(TInputStreamWrap* base)
    : TGrowingStreamStack(base)
{
    THREAD_AFFINITY_IS_V8();
    YASSERT(base == Bottom());
    GetBaseStream()->AsyncRef(true);
}

TNodeJSInputStack::~TNodeJSInputStack() throw()
{
    GetBaseStream()->AsyncUnref();
}

TInputStreamWrap* TNodeJSInputStack::GetBaseStream()
{
    return static_cast<TInputStreamWrap*>(Bottom());
}

void TNodeJSInputStack::AddCompression(ECompression compression)
{
    AddCompressionToStack(*this, compression);
}

size_t TNodeJSInputStack::DoRead(void* data, size_t length)
{
    return Top()->Read(data, length);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
