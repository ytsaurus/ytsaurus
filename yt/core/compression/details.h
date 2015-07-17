#pragma once

#include <core/misc/ref.h>
#include <core/misc/serialize.h>

#include <core/actions/callback.h>

#include <core/logging/log.h>

#include <contrib/libs/snappy/snappy.h>
#include <contrib/libs/snappy/snappy-sinksource.h>

#include <vector>

namespace NYT {
namespace NCompression {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger CompressionLogger;

////////////////////////////////////////////////////////////////////////////////

typedef snappy::Sink StreamSink;
typedef snappy::Source StreamSource;
typedef snappy::ByteArraySource ByteArraySource;

typedef std::function<void (StreamSource*, TBlob*)> TConverter;

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

inline void Read(StreamSource* source, char* data, size_t len)
{
    YCHECK(source->Available() >= len);
    size_t current = 0;
    do {
        size_t inputLen;
        const char* input = source->Peek(&inputLen);
        size_t copyLen = std::min(len - current, inputLen);
        std::copy(input, input + copyLen , data + current);
        source->Skip(copyLen);
        current += copyLen;
    } while (current < len);
}

inline void Read(StreamSource* source, TBlob& output)
{
    output.Reserve(source->Available());
    while (source->Available() > 0) {
        size_t inputLen;
        const char* input = source->Peek(&inputLen);
        output.Append(input, inputLen);
        source->Skip(inputLen);
    }
}

template <class T>
void ReadPod(StreamSource* source, T& value)
{
    YCHECK(source->Available() >= sizeof(T));
    
    char data[sizeof(T)];
    for (size_t i = 0; i < sizeof(T); ++i) {
        size_t len;
        data[i] = *(source->Peek(&len));
        source->Skip(1);
    }

    TMemoryInput memoryInput(data, sizeof(T));
    NYT::ReadPod(memoryInput, value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCompresssionCodec
} // namespace NYT
