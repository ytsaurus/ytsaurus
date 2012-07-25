#include "stdafx.h"
#include "codec.h"

#include "perform_convertion.h"
#include "snappy.h"
#include "zlib.h"
#include "lz.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TNoneCodec
    : public ICodec
{
public:
    virtual TSharedRef Compress(const TSharedRef& block) OVERRIDE
    {
        return block;
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) OVERRIDE
    {
        return MergeRefs(blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) OVERRIDE
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
        return Perform(block, BIND(SnappyCompress));
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) OVERRIDE
    {
        return Perform(blocks, BIND(SnappyCompress));
    }

    virtual TSharedRef Decompress(const TSharedRef& block) OVERRIDE
    {
        return Perform(block, BIND(SnappyDecompress));
    }

};

////////////////////////////////////////////////////////////////////////////////

class TGzipCodec
    : public ICodec
{
public:
    virtual TSharedRef Compress(const TSharedRef& block) OVERRIDE
    {
        return Perform(block, BIND(ZlibCompress));
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) OVERRIDE
    {
        return Perform(blocks, BIND(ZlibCompress));
    }

    virtual TSharedRef Decompress(const TSharedRef& block) OVERRIDE
    {
        return Perform(block, BIND(ZlibDecompress));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TLz4Codec
    : public ICodec
{
public:
    virtual TSharedRef Compress(const TSharedRef& block) OVERRIDE
    {
        return Perform(block, BIND(Lz4Compress));
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) OVERRIDE
    {
        return Perform(blocks, BIND(Lz4Compress));
    }

    virtual TSharedRef Decompress(const TSharedRef& block) OVERRIDE
    {
        return Perform(block, BIND(Lz4Decompress));
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

        case ECodecId::Gzip:
            return Singleton<TGzipCodec>();

        case ECodecId::Lz4:
            return Singleton<TLz4Codec>();

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

