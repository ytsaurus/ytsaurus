#include "output_stack.h"

#include <yt/core/utilex/lzop.h>

#include <library/streams/lz/lz.h>

#include <util/stream/zlib.h>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

TNodeJSOutputStack::TNodeJSOutputStack(TOutputStreamWrap* base)
    : TGrowingStreamStack(base)
{
    THREAD_AFFINITY_IS_V8();
    YASSERT(Bottom() == base);
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

void TNodeJSOutputStack::DoWrite(const void* buffer, size_t length)
{
    Top()->Write(buffer, length);
}

void TNodeJSOutputStack::DoWriteV(const TPart* parts, size_t count)
{
    Top()->Write(parts, count);
}

void TNodeJSOutputStack::DoFlush()
{
    Top()->Flush();
}

void TNodeJSOutputStack::DoFinish()
{
    GetBaseStream()->MarkAsFinishing();

    for (auto* current : *this) {
        current->Finish();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
