#include "serialize.h"

#include <yt/yt/core/concurrency/fls.h>

#include <library/cpp/yt/assert/assert.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

const std::array<ui8, ZeroBufferSize> ZeroBuffer{};
static NConcurrency::TFls<int> CrashOnErrorDepth;

////////////////////////////////////////////////////////////////////////////////

void AssertSerializationAligned(i64 byteSize)
{
    YT_ASSERT(AlignUpSpace<i64>(byteSize, SerializationAlignment) == 0);
}

void VerifySerializationAligned(i64 byteSize)
{
    YT_VERIFY(AlignUpSpace<i64>(byteSize, SerializationAlignment) == 0);
}

void MakeSerializationAligned(char** buffer, i64 byteSize)
{
    auto paddingSize = AlignUpSpace<i64>(byteSize, SerializationAlignment);
    memset(*buffer, 0, paddingSize);
    *buffer += paddingSize;
}

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

