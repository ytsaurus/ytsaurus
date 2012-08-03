#include "stdafx.h"
#include "codec.h"
#include "helpers.h"
#include "snappy.h"
#include "zlib.h"
#include "lz.h"

namespace NYT {
namespace NCodec {

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
        return NCodec::Apply(BIND(NCodec::SnappyCompress), block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) OVERRIDE
    {
        return NCodec::Apply(BIND(NCodec::SnappyCompress), blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) OVERRIDE
    {
        return NCodec::Apply(BIND(NCodec::SnappyDecompress), block);
    }

};

////////////////////////////////////////////////////////////////////////////////

class TGzipCodec
    : public ICodec
{
public:
    explicit TGzipCodec(int level)
        : Compressor_(BIND(NCodec::ZlibCompress, level))
    { }

    virtual TSharedRef Compress(const TSharedRef& block) OVERRIDE
    {
        return NCodec::Apply(Compressor_, block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) OVERRIDE
    {
        return NCodec::Apply(Compressor_, blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) OVERRIDE
    {
        return NCodec::Apply(BIND(NCodec::ZlibDecompress), block);
    }

private:
    NCodec::TConverter Compressor_;
};

////////////////////////////////////////////////////////////////////////////////

class TLz4Codec
    : public ICodec
{
public:
    explicit TLz4Codec(bool highCompression)
        : Compressor_(BIND(NCodec::Lz4Compress, highCompression))
    { }

    virtual TSharedRef Compress(const TSharedRef& block) OVERRIDE
    {
        return NCodec::Apply(Compressor_, block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) OVERRIDE
    {
        return NCodec::Apply(Compressor_, blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) OVERRIDE
    {
        return NCodec::Apply(BIND(NCodec::Lz4Decompress), block);
    }

private:
    NCodec::TConverter Compressor_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCodec
} // namespace NYT


namespace NYT {

TCodecPtr GetCodec(ECodecId id)
{
    switch (id) {
        case ECodecId::None:
            return New<NCodec::TNoneCodec>();

        case ECodecId::Snappy:
            return New<NCodec::TSnappyCodec>();

        case ECodecId::GzipNormal:
            return New<NCodec::TGzipCodec>(6);

        case ECodecId::GzipBestCompression:
            return New<NCodec::TGzipCodec>(9);

        case ECodecId::Lz4:
            return New<NCodec::TLz4Codec>(false);

        case ECodecId::Lz4HighCompression:
            return New<NCodec::TLz4Codec>(true);

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

