#pragma once

#include <ytlib/misc/ref.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/actions/callback.h>

#include <contrib/libs/snappy/snappy.h>
#include <contrib/libs/snappy/snappy-sinksource.h>

#include <vector>

namespace NYT {
namespace NCodec {

////////////////////////////////////////////////////////////////////////////////

typedef snappy::Sink StreamSink;
typedef snappy::Source StreamSource;
typedef snappy::ByteArraySource ByteArraySource;

size_t TotalLength(const std::vector<TSharedRef>& refs);
TSharedRef MergeRefs(const std::vector<TSharedRef>& refs);

typedef TCallback<void (StreamSource*, std::vector<char>*)> TConverter;

//TODO(ignat): rename these methods
TSharedRef Apply(TConverter converter, const TSharedRef& ref);
TSharedRef Apply(TConverter converter, const std::vector<TSharedRef>& refs);

////////////////////////////////////////////////////////////////////////////////

//! Implements snappy::Source interface over a vector of TSharedRef-s. 
class VectorRefsSource
    : public StreamSource
{
public:
    explicit VectorRefsSource(const std::vector<TSharedRef>& blocks);

    virtual size_t Available() const OVERRIDE;

    virtual const char* Peek(size_t* len) OVERRIDE;

    virtual void Skip(size_t n) OVERRIDE;

private:
    void SkipCompletedBlocks();

    const std::vector<TSharedRef>& Blocks_;
    size_t Available_;
    size_t Index_;
    size_t Position_;
};

////////////////////////////////////////////////////////////////////////////////

class DynamicByteArraySink
    : public StreamSink
{
public:
    explicit DynamicByteArraySink(std::vector<char>* output);

    virtual void Append(const char* data, size_t n) OVERRIDE;

private:
    std::vector<char>* Output_;
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

template<class T>
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

} // namespace NCodec
} // namespace NYT
