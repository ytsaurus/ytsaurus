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
        return Apply(BIND(SnappyCompress), block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) OVERRIDE
    {
        return Apply(BIND(SnappyCompress), blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) OVERRIDE
    {
        return Apply(BIND(SnappyDecompress), block);
    }

};

////////////////////////////////////////////////////////////////////////////////

class TGzipCodec
    : public ICodec
{
public:
    virtual TSharedRef Compress(const TSharedRef& block) OVERRIDE
    {
        return Apply(BIND(ZlibCompress), block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) OVERRIDE
    {
        return Apply(BIND(ZlibCompress), blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) OVERRIDE
    {
        return Apply(BIND(ZlibDecompress), block);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TLz4Codec
    : public ICodec
{
public:
    virtual TSharedRef Compress(const TSharedRef& block) OVERRIDE
    {
        return Apply(BIND(Lz4Compress), block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) OVERRIDE
    {
        return Apply(BIND(Lz4Compress), blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) OVERRIDE
    {
        return Apply(BIND(Lz4Decompress), block);
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

