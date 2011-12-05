#include "stdafx.h"
#include "codec.h"

#include "assert.h"

#include <contrib/libs/snappy/snappy.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TNoneCodec
    : public ICodec
{
public:
    virtual TSharedRef Compress(const TSharedRef& block)
    {
        return block;
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