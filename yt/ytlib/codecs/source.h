#pragma once

#include "shared_ref_helpers.h"

#include <ytlib/misc/ref.h>
#include <ytlib/misc/serialize.h>

#include <util/stream/mem.h>

#include <contrib/libs/snappy/snappy.h>
#include <contrib/libs/snappy/snappy-sinksource.h>

#include <vector>

namespace NYT {

typedef snappy::Sink StreamSink;
typedef snappy::Source StreamSource;
typedef snappy::ByteArraySource ByteArraySource;

//! Implements snappy::Source interface over a vector of TSharedRef-s. 
class VectorRefsSource
    : public StreamSource
{
public:
    explicit VectorRefsSource(const std::vector<TSharedRef>& blocks)
        : Blocks_(blocks)
        , Available_(TotalLength(blocks))
        , Index_(0)
        , Position_(0)
    {
        SkipCompletedBlocks();
    }

    virtual size_t Available() const OVERRIDE
    {
        return Available_;
    }

    virtual const char* Peek(size_t* len) OVERRIDE
    {
        if (Index_ == Blocks_.size()) {
            *len = 0;
            return NULL;
        }
        *len = Blocks_[Index_].Size() - Position_;
        return Blocks_[Index_].Begin() + Position_;
    }

    virtual void Skip(size_t n) OVERRIDE
    {
        while (n > 0 && Index_ < Blocks_.size()) {
            size_t toSkip = std::min(Blocks_[Index_].Size() - Position_, n);

            Position_ += toSkip;
            SkipCompletedBlocks();

            n -= toSkip;
            Available_ -= toSkip;
        }
    }

private:
    void SkipCompletedBlocks()
    {
        while (Index_ < Blocks_.size() && Position_ == Blocks_[Index_].Size()) {
            Index_ += 1;
            Position_ = 0;
        }
    }

    const std::vector<TSharedRef>& Blocks_;
    size_t Available_;
    size_t Index_;
    size_t Position_;
};

class DynamicByteArraySink
    : public StreamSink
{
public:
    explicit DynamicByteArraySink(std::vector<char>* output)
        : Output_(output)
    { }

    virtual void Append(const char* data, size_t n) OVERRIDE
    {
        for (size_t i = 0; i < n; ++i) {
            Output_->push_back(data[i]);
        }
    }

private:
    std::vector<char>* Output_;
};

template<class T>
void ReadPod(StreamSource* source, T& value) {
    YCHECK(source->Available() >= sizeof(T));
    char data[sizeof(T)];
    for (size_t i = 0; i < sizeof(T); ++i) {
        size_t len;
        data[i] = *(source->Peek(&len));
        source->Skip(1);
    }
    TMemoryInput memoryInput(data, sizeof(T));

    ReadPod(memoryInput, value);
}

inline void Read(StreamSource* source, char* data, size_t len) {
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

} // namespace NYT


