#include "input_stack.h"

#include <util/stream/zlib.h>
#include <util/stream/lz.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TNodeJSInputStack::TNodeJSInputStack(TNodeJSInputStream* base)
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

TNodeJSInputStream* TNodeJSInputStack::GetBaseStream()
{
    return static_cast<TNodeJSInputStream*>(Bottom());
}

void TNodeJSInputStack::AddCompression(ECompression compression)
{
    switch (compression) {
        case ECompression::None:
            break;
        case ECompression::Gzip:
        case ECompression::Deflate:
            Add<TZLibDecompress>();
            break;
        case ECompression::LZO:
            Add<TLzoDecompress>();
            break;
        case ECompression::LZF:
            Add<TLzfDecompress>();
            break;
        case ECompression::Snappy:
            Add<TSnappyDecompress>();
            break;
        default:
            YUNREACHABLE();  
    }
}

size_t TNodeJSInputStack::DoRead(void* data, size_t length)
{
    return Top()->Read(data, length);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
