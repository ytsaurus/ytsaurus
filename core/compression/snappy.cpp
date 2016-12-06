#include "snappy.h"
#include "details.h"

#include <contrib/libs/snappy/snappy-stubs-internal.h>
#include <contrib/libs/snappy/snappy.h>

namespace NYT {
namespace NCompression {

////////////////////////////////////////////////////////////////////////////////

class TPreloadingSource
    : public StreamSource
{
public:
    explicit TPreloadingSource(StreamSource* source)
        : Source_(source)
        , Length_(std::min(Source_->Available(), Buffer_.size()))
    {
        Read(Source_, Buffer_.data(), Length_);
    }

    virtual size_t Available() const override
    {
        return Source_->Available() + Length_ - Position_;
    }

    virtual const char* Peek(size_t* length) override
    {
        if (Y_UNLIKELY(Position_ < Length_)) {
            *length = Length_ - Position_;
            return Buffer_.begin() + Position_;
        } else {
            return Source_->Peek(length);
        }
    }

    virtual void Skip(size_t length) override
    {
        if (Y_UNLIKELY(Position_ < Length_)) {
            auto delta = std::min(length, Length_ - Position_);
            Position_ += delta;
            length -= delta;
        }
        if (length > 0) {
            Source_->Skip(length);
        }
    }

    const char* begin() const
    {
        return Buffer_.begin();
    }

    const char* end() const
    {
        return Buffer_.begin() + Length_;
    }

private:
    StreamSource* Source_;
    std::array<char, snappy::Varint::kMax32> Buffer_;
    size_t Length_ = 0;
    size_t Position_ = 0;
};

void SnappyCompress(StreamSource* source, TBlob* output)
{
    // Snappy implementation relies on entire input length to fit into an integer.
    YCHECK(source->Available() <= std::numeric_limits<int>::max());

    output->Resize(snappy::MaxCompressedLength(source->Available()), false);
    snappy::UncheckedByteArraySink writer(output->Begin());
    size_t compressedSize = snappy::Compress(source, &writer);
    output->Resize(compressedSize);
}

void SnappyDecompress(StreamSource* source, TBlob* output)
{
    // Empty input leads to an empty output in Snappy.
    if (source->Available() == 0) {
        return;
    }

    // We hack into Snappy implementation to preallocate appropriate buffer.
    ui32 uncompressedSize = 0;
    TPreloadingSource preloadingSource(source);
    snappy::Varint::Parse32WithLimit(
        preloadingSource.begin(),
        preloadingSource.end(),
        &uncompressedSize);
    output->Resize(uncompressedSize, false);
    YCHECK(snappy::RawUncompress(&preloadingSource, output->Begin()));
}

////////////////////////////////////////////////////////////////////////////////

}} // namespace NYT::NCompression

