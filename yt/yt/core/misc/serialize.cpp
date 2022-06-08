#include "serialize.h"

#include <yt/yt/core/concurrency/fls.h>

#include <library/cpp/yt/assert/assert.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

const std::array<ui8, ZeroBufferSize> ZeroBuffer{};
static NConcurrency::TFls<int> CrashOnErrorDepth;

////////////////////////////////////////////////////////////////////////////////

TCrashOnDeserializationErrorGuard::TCrashOnDeserializationErrorGuard()
{
    ++*CrashOnErrorDepth;
}

TCrashOnDeserializationErrorGuard::~TCrashOnDeserializationErrorGuard()
{
    YT_VERIFY(--*CrashOnErrorDepth >= 0);
}

void TCrashOnDeserializationErrorGuard::OnError()
{
    YT_VERIFY(*CrashOnErrorDepth == 0);
}

////////////////////////////////////////////////////////////////////////////////

TStreamSaveContext::TStreamSaveContext()
    : Output_(nullptr)
{ }

TStreamSaveContext::TStreamSaveContext(IOutputStream* output)
    : Output_(output)
{ }

////////////////////////////////////////////////////////////////////////////////

TStreamLoadContext::TStreamLoadContext()
    : Input_(nullptr)
{ }

TStreamLoadContext::TStreamLoadContext(IInputStream* input)
    : Input_(input)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

