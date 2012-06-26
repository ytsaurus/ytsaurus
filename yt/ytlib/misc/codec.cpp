#include "stdafx.h"
#include "codec.h"
#include "foreach.h"

#include <contrib/libs/snappy/snappy.h>
#include <contrib/libs/snappy/snappy-sinksource.h>

namespace NYT {

namespace  {

size_t Size(const std::vector<TSharedRef>& refs)
{
    size_t size = 0;
    FOREACH(const auto& ref, refs) {
        size += ref.Size();
    }
    return size;
}

//! Implements snappy::Source interface over a vector of TSharedRef-s. 
class VectorRefsSource:
    public snappy::Source
{
public:
    VectorRefsSource(const std::vector<TSharedRef>& blocks):
        Blocks_(blocks),
        Available_(Size(blocks)),
        Index_(0),
        Position_(0)
    { }

    virtual size_t Available() const OVERRIDE
    {
        return Available_;
    }

    virtual const char* Peek(size_t* len) OVERRIDE
    {
        *len = Blocks_[Index_].Size() - Position_;
        return Blocks_[Index_].Begin() + Position_;
    }

    virtual void Skip(size_t n) OVERRIDE
    {
        while (n > 0 && Index_ < Blocks_.size()) {
            size_t toSkip = std::min(Blocks_[Index_].Size() - Position_, n);

            Position_ += toSkip;
            if (Position_ == Blocks_[Index_].Size()) {
                Index_ += 1;
                Position_ = 0;
            }

            n -= toSkip;
            Available_ -= toSkip;
        }
    }

private:
    const std::vector<TSharedRef>& Blocks_;
    size_t Available_;
    size_t Index_;
    size_t Position_;
};

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////

class TNoneCodec
    : public ICodec
{
public:
    virtual TSharedRef Compress(const TSharedRef& block)
    {
        return block;
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks)
    {
        TBlob result(Size(blocks));
        size_t pos = 0;
        for (size_t i = 0; i < blocks.size(); ++i) {
            std::copy(blocks[i].Begin(), blocks[i].End(), result.begin() + pos);
            pos += blocks[i].Size();
        }
        return TSharedRef(MoveRV(result));
    }

    virtual TSharedRef Decompress(const TSharedRef& block)
    {
        return block;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSnappyCodec
    : public ICodec
{
public:
    virtual TSharedRef Compress(const TSharedRef& block) OVERRIDE
    {
        auto maxSize = snappy::MaxCompressedLength(block.Size());
        TBlob blob(maxSize);
        size_t compressedSize;
        snappy::RawCompress(block.Begin(), block.Size(), blob.begin(), &compressedSize);
        TRef ref(blob.begin(), compressedSize);
        return TSharedRef(MoveRV(blob), ref);
    }

    //! Compress vector of blocks without efficiently,
    //! without memory copying.
    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) OVERRIDE
    {
        if (blocks.size() == 1) {
            return Compress(blocks.front());
        }

        VectorRefsSource reader(blocks);
        TBlob result(snappy::MaxCompressedLength(reader.Available()));

        snappy::UncheckedByteArraySink writer(&*result.begin());
        snappy::Compress(&reader, &writer);

        size_t compressedLength = writer.CurrentDestination() - &(*result.begin());
        TRef compressedRef(&*result.begin(), compressedLength);
        return TSharedRef(MoveRV(result), compressedRef);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) OVERRIDE  
    {
        size_t size = 0;
        YVERIFY(snappy::GetUncompressedLength(block.Begin(), block.Size(), &size));
        TBlob blob(size);
        snappy::RawUncompress(block.Begin(), block.Size(), blob.begin());
        return TSharedRef(MoveRV(blob));
    }

};

////////////////////////////////////////////////////////////////////////////////

ICodec* GetCodec(ECodecId id)
{
    switch (id) {
        case ECodecId::None:
            return Singleton<TNoneCodec>();

        case ECodecId::Snappy:
            return Singleton<TSnappyCodec>();

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

