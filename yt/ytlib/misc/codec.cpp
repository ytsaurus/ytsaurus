#include "stdafx.h"
#include "codec.h"

#include "assert.h"

#include <contrib/libs/snappy/snappy.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TTrivialCodec
    : public ICodec
{
public:
    virtual TSharedRef Encode(const TSharedRef& block) const
    {
        return block;
    }

    virtual TSharedRef Decode(const TSharedRef& block) const
    {
        return block;
    }

    static const TTrivialCodec* GetInstance()
    {
        return Singleton<TTrivialCodec>();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSnappyCodec
    : public ICodec
{
public:
    virtual TSharedRef Encode(const TSharedRef& block) const
    {
        auto maxSize = snappy::MaxCompressedLength(block.Size());
        TBlob blob(maxSize);
        size_t compressedSize;
        snappy::RawCompress(block.Begin(), block.Size(), blob.begin(), &compressedSize);
        TRef ref(blob.begin(), compressedSize);
        return TSharedRef(MoveRV(blob), ref);
    }

    virtual TSharedRef Decode(const TSharedRef& block) const
    {
        size_t size = 0;
        YVERIFY(snappy::GetUncompressedLength(block.Begin(), block.Size(), &size));
        TBlob blob(size);
        snappy::RawUncompress(block.Begin(), block.Size(), blob.begin());
        return TSharedRef(MoveRV(blob));
    }

    static const TSnappyCodec* GetInstance()
    {
        return Singleton<TSnappyCodec>();
    }
};

////////////////////////////////////////////////////////////////////////////////

const ICodec& ICodec::GetCodec(ECodecId id)
{
    switch (id) {
        case ECodecId::None:
            return *TTrivialCodec::GetInstance();

        case ECodecId::Snappy:
            return *TSnappyCodec::GetInstance();

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT