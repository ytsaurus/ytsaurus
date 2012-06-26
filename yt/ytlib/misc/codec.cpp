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

//! Implements special case of Source for vector if shared refs.
//! Prevents excess memory copying.
class VectorRefsSource: public snappy::Source {
public:
    VectorRefsSource(const std::vector<TSharedRef>& blocks):
        blocks_(blocks),
        available_(Size(blocks)),
        index_(0),
        position_(0)
    { }

    virtual ~VectorRefsSource()
    { }

    virtual size_t Available() const
    {
        return available_;
    }

    virtual const char* Peek(size_t* len)
    {
        *len = blocks_[index_].Size() - position_;
        return blocks_[index_].Begin() + position_;
    }

    virtual void Skip(size_t n)
    {
        while (n > 0 && index_ < blocks_.size()) {
            size_t toSkip = std::min(blocks_[index_].Size() - position_, n);

            position_ += toSkip;
            if (position_ == blocks_[index_].Size()) {
                index_ += 1;
                position_ = 0;
            }

            n -= toSkip;
            available_ -= toSkip;
        }
    }

private:
    const std::vector<TSharedRef>& blocks_;
    size_t available_;
    size_t index_;
    size_t position_;
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
            std::memcpy(&(*result.begin()) + pos, blocks[i].Begin(), blocks[i].Size());
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
    virtual TSharedRef Compress(const TSharedRef& block)
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
    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks)
    {
        VectorRefsSource reader(blocks);
        yvector<char> result(snappy::MaxCompressedLength(reader.Available()));

        snappy::UncheckedByteArraySink writer(&(*result.begin()));
        snappy::Compress(&reader, &writer);

        size_t compressedLength = writer.CurrentDestination() - &(*result.begin());
        TRef compressedRef(&(*result.begin()), compressedLength);
        return TSharedRef(MoveRV(result), compressedRef);
    }

    virtual TSharedRef Decompress(const TSharedRef& block)
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
