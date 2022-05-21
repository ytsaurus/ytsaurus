#include "serialize.h"

#include <library/cpp/yt/assert/assert.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

const std::array<ui8, ZeroBufferSize> ZeroBuffer{};

////////////////////////////////////////////////////////////////////////////////

TCrashOnDeserializationErrorGuard::TCrashOnDeserializationErrorGuard()
{
    ++CrashOnErrorDepth_;
}

TCrashOnDeserializationErrorGuard::~TCrashOnDeserializationErrorGuard()
{
    YT_VERIFY(--CrashOnErrorDepth_ >= 0);
}

void TCrashOnDeserializationErrorGuard::OnError()
{
    YT_VERIFY(CrashOnErrorDepth_ == 0);
}

thread_local int TCrashOnDeserializationErrorGuard::CrashOnErrorDepth_;

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

