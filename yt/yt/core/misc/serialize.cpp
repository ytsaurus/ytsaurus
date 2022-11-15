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

TSaveContextStream::TSaveContextStream(IOutputStream* output)
    : BufferedOutput_(output)
    , Output_(&*BufferedOutput_)
{ }

TSaveContextStream::TSaveContextStream(IZeroCopyOutput* output)
    : Output_(output)
{ }

void TSaveContextStream::Flush()
{
    Output_->Undo(BufferSize_);
    BufferPtr_ = nullptr;
    BufferSize_ = 0;
    Output_->Flush();
    if (BufferedOutput_) {
        BufferedOutput_->Flush();
    }
}

void TSaveContextStream::WriteSlow(const void* buf, size_t len)
{
    while (len > 0) {
        if (BufferSize_ == 0) {
            BufferSize_ = Output_->Next(&BufferPtr_);
        }
        YT_ASSERT(BufferSize_ > 0);
        auto toCopy = std::min(len, BufferSize_);
        ::memcpy(BufferPtr_, buf, toCopy);
        BufferPtr_ += toCopy;
        BufferSize_ -= toCopy;
        buf = static_cast<const char*>(buf) + toCopy;
        len -= toCopy;
    }
}

////////////////////////////////////////////////////////////////////////////////

TStreamSaveContext::TStreamSaveContext(
    IOutputStream* output,
    int version)
    : Output_(output)
    , Version_(version)
{ }

TStreamSaveContext::TStreamSaveContext(
    IZeroCopyOutput* output,
    int version)
    : Output_(output)
    , Version_(version)
{ }

void TStreamSaveContext::Finish()
{
    Output_.Flush();
}

////////////////////////////////////////////////////////////////////////////////

TStreamLoadContext::TStreamLoadContext()
    : Input_(nullptr)
{ }

TStreamLoadContext::TStreamLoadContext(IInputStream* input)
    : Input_(input)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

