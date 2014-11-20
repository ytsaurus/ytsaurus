#include "output_stack.h"

#include <util/stream/zlib.h>
#include <util/stream/lz.h>
#include <util/stream/lzop.h>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

TNodeJSOutputStack::TNodeJSOutputStack(TOutputStreamWrap* base)
    : TGrowingStreamStack(base)
    , HasAnyData_(false)
{
    THREAD_AFFINITY_IS_V8();
    YASSERT(base == Bottom());
    GetBaseStream()->AsyncRef(true);
}

TNodeJSOutputStack::~TNodeJSOutputStack() throw()
{
    GetBaseStream()->AsyncUnref();
}

TOutputStreamWrap* TNodeJSOutputStack::GetBaseStream()
{
    return static_cast<TOutputStreamWrap*>(Bottom());
}

void TNodeJSOutputStack::AddCompression(ECompression compression)
{
    AddCompressionToStack(*this, compression);
}

bool TNodeJSOutputStack::HasAnyData()
{
    return HasAnyData_;
}

void TNodeJSOutputStack::DoWrite(const void* buffer, size_t length)
{
    HasAnyData_ = true;
    return Top()->Write(buffer, length);
}

void TNodeJSOutputStack::DoWriteV(const TPart* parts, size_t count)
{
    return Top()->Write(parts, count);
}

void TNodeJSOutputStack::DoFlush()
{
    return Top()->Flush();
}

void TNodeJSOutputStack::DoFinish()
{
    GetBaseStream()->SetCompleted();

    for (auto* current : *this) {
        current->Finish();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
