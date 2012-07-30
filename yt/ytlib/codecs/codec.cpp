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
    explicit TGzipCodec(int level)
        : Compressor_(
            BIND([=] (StreamSource* source, std::vector<char>* output)
                    {ZlibCompress(source, output, level);}))
    { }

    virtual TSharedRef Compress(const TSharedRef& block) OVERRIDE
    {
        return Apply(Compressor_, block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) OVERRIDE
    {
        return Apply(Compressor_, blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) OVERRIDE
    {
        return Apply(BIND(ZlibDecompress), block);
    }

private:
    TConverter Compressor_;
};

////////////////////////////////////////////////////////////////////////////////

class TLz4Codec
    : public ICodec
{
public:
    explicit TLz4Codec(bool highCompression)
        : Compressor_(
            BIND([=] (StreamSource* source, std::vector<char>* output)
                    {Lz4Compress(source, output, highCompression);}))
    { }

    virtual TSharedRef Compress(const TSharedRef& block) OVERRIDE
    {
        return Apply(Compressor_, block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) OVERRIDE
    {
        return Apply(Compressor_, blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) OVERRIDE
    {
        return Apply(BIND(Lz4Decompress), block);
    }

private:
    TConverter Compressor_;
};

////////////////////////////////////////////////////////////////////////////////

TCodecPtr GetCodec(ECodecId id)
{
    switch (id) {
        case ECodecId::None:
            return New<TNoneCodec>();

        case ECodecId::Snappy:
            return New<TSnappyCodec>();

        case ECodecId::GzipNormal:
            return New<TGzipCodec>(6);

        case ECodecId::GzipBestCompression:
            return New<TGzipCodec>(9);

        case ECodecId::Lz4:
            return New<TLz4Codec>(false);

        case ECodecId::Lz4HighCompression:
            return New<TLz4Codec>(true);

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

