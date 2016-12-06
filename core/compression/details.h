#pragma once

#include <yt/core/logging/log.h>

#include <yt/core/misc/ref.h>
#include <yt/core/misc/serialize.h>

#include <contrib/libs/snappy/snappy-sinksource.h>
#include <contrib/libs/snappy/snappy.h>

#include <array>
#include <vector>

namespace NYT {
namespace NCompression {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger CompressionLogger;

////////////////////////////////////////////////////////////////////////////////

typedef snappy::Sink StreamSink;
typedef snappy::Source StreamSource;
typedef snappy::ByteArraySource ByteArraySource;

////////////////////////////////////////////////////////////////////////////////

//! Implements snappy::Source interface over a vector of TSharedRef-s.
class TVectorRefsSource
    : public StreamSource
{
public:
    explicit TVectorRefsSource(const std::vector<TSharedRef>& blocks);

    virtual size_t Available() const override;

    virtual const char* Peek(size_t* len) override;

    virtual void Skip(size_t n) override;

private:
    void SkipCompletedBlocks();

    const std::vector<TSharedRef>& Blocks_;
    size_t Available_;
    size_t Index_;
    size_t Position_;
};

////////////////////////////////////////////////////////////////////////////////

class TDynamicByteArraySink
    : public StreamSink
{
public:
    explicit TDynamicByteArraySink(TBlob* output);

    virtual void Append(const char* data, size_t n) override;

private:
    TBlob* Output_;
};

////////////////////////////////////////////////////////////////////////////////

inline void Read(StreamSource* source, char* buffer, size_t length)
{
    YCHECK(source->Available() >= length);
    size_t offset = 0;
    do {
        size_t inputLength;
        const char* input = source->Peek(&inputLength);

        inputLength = std::min(length - offset, inputLength);
        std::copy(input, input + inputLength, buffer + offset);

        source->Skip(inputLength);
        offset += inputLength;
    } while (offset < length);
}

inline void Read(StreamSource* source, TBlob& output)
{
    size_t length = source->Available();
    output.Resize(length);
    Read(source, output.Begin(), length);
}

template <class T>
void ReadPod(StreamSource* source, T& value)
{
    Read(source, reinterpret_cast<char*>(&value), sizeof(T));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCompresssionCodec
} // namespace NYT
