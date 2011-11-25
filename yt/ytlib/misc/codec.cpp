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

    virtual TCodecId GetId() const
    {
        return 0;
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

    virtual TCodecId GetId() const
    {
        return 1;
    }

    static const TSnappyCodec* GetInstance()
    {
        return Singleton<TSnappyCodec>();
    }
};

////////////////////////////////////////////////////////////////////////////////

const ICodec& ICodec::GetCodec(TCodecId id)
{
    switch (id) {
        case 0:
            return *TTrivialCodec::GetInstance();

        case 1:
            return *TSnappyCodec::GetInstance();

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT