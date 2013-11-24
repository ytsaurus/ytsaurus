#include "stdafx.h"
#include "codec.h"
#include "helpers.h"
#include "snappy.h"
#include "zlib.h"
#include "lz.h"

namespace NYT {
namespace NCompression {

struct TCompressedBlockTag { };

struct TDecompressedBlockTag { };

////////////////////////////////////////////////////////////////////////////////

int ZeroFunction(const std::vector<int>&)
{
    return 0;
}

//TODO(ignat): rename these methods
template <class TBlockTag>
TSharedRef Apply(TConverter converter, const TSharedRef& ref)
{
    ByteArraySource source(ref.Begin(), ref.Size());
    TBlob output;
    converter.Run(&source, &output);
    return TSharedRef::FromBlob<TBlockTag>(std::move(output));
}

template <class TBlockTag>
TSharedRef Apply(
    TConverter converter,
    const std::vector<TSharedRef>& refs,
    std::function<int(const std::vector<int>)> outputSizeEstimator = ZeroFunction)
{
    if (refs.size() == 1) {
        return Apply<TBlockTag>(converter, refs.front());
    }
    TVectorRefsSource source(refs);

    std::vector<int> lengths;
    for (const auto& ref: refs) {
        lengths.push_back(ref.Size());
    }
    
    TBlob output;
    output.Reserve(outputSizeEstimator(lengths));

    converter.Run(&source, &output);
    return TSharedRef::FromBlob<TBlockTag>(std::move(output));
}

////////////////////////////////////////////////////////////////////////////////

class TNoneCodec
    : public ICodec
{
public:
    virtual TSharedRef Compress(const TSharedRef& block) override
    {
        return block;
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) override
    {
        return MergeRefs(blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) override
    {
        return block;
    }

    virtual ECodec GetId() const override
    {
        return ECodec::None;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSnappyCodec
    : public ICodec
{
public:
    virtual TSharedRef Compress(const TSharedRef& block) override
    {
        return Apply<TCompressedBlockTag>(BIND(NCompression::SnappyCompress), block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) override
    {
        return Apply<TCompressedBlockTag>(BIND(NCompression::SnappyCompress), blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) override
    {
        return Apply<TDecompressedBlockTag>(BIND(NCompression::SnappyDecompress), block);
    }

    virtual ECodec GetId() const override
    {
        return ECodec::Snappy;
    }

};

////////////////////////////////////////////////////////////////////////////////

class TGzipCodec
    : public ICodec
{
public:
    explicit TGzipCodec(int level)
        : Compressor_(BIND(NCompression::ZlibCompress, level))
        , Level_(level)
    { }

    virtual TSharedRef Compress(const TSharedRef& block) override
    {
        return Apply<TCompressedBlockTag>(Compressor_, block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) override
    {
        return Apply<TCompressedBlockTag>(Compressor_, blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) override
    {
        return Apply<TDecompressedBlockTag>(BIND(NCompression::ZlibDecompress), block);
    }

    virtual ECodec GetId() const override
    {
        if (Level_ == 6) {
            return ECodec::GzipNormal;
        }
        if (Level_ == 9) {
            return ECodec::GzipBestCompression;
        }
        YUNREACHABLE();
    }

private:
    NCompression::TConverter Compressor_;

    int Level_;
};

////////////////////////////////////////////////////////////////////////////////

class TLz4Codec
    : public ICodec
{
public:
    explicit TLz4Codec(bool highCompression)
        : Compressor_(BIND(NCompression::Lz4Compress, highCompression))
        , CodecId_(highCompression ? ECodec::Lz4HighCompression : ECodec::Lz4)
    { }

    virtual TSharedRef Compress(const TSharedRef& block) override
    {
        return Apply<TCompressedBlockTag>(Compressor_, block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) override
    {
        return Apply<TCompressedBlockTag>(Compressor_, blocks, Lz4CompressionBound);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) override
    {
        return Apply<TDecompressedBlockTag>(BIND(NCompression::Lz4Decompress), block);
    }

    virtual ECodec GetId() const override
    {
        return CodecId_;
    }

private:
    NCompression::TConverter Compressor_;

    ECodec CodecId_;
};

////////////////////////////////////////////////////////////////////////////////

class TQuickLzCodec
    : public ICodec
{
public:
    explicit TQuickLzCodec()
        : Compressor_(BIND(NCompression::QuickLzCompress))
    { }

    virtual TSharedRef Compress(const TSharedRef& block) override
    {
        return Apply<TCompressedBlockTag>(Compressor_, block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) override
    {
        return Apply<TCompressedBlockTag>(Compressor_, blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) override
    {
        return Apply<TDecompressedBlockTag>(BIND(NCompression::QuickLzDecompress), block);
    }

    virtual ECodec GetId() const override
    {
        return ECodec::QuickLz;
    }

private:
    NCompression::TConverter Compressor_;
};

////////////////////////////////////////////////////////////////////////////////

ICodec* GetCodec(ECodec id)
{
    switch (id) {
        case ECodec::None: {
            static TNoneCodec result;
            return &result;
        }
        case ECodec::Snappy: {
            static TSnappyCodec result;
            return &result;
        }

        case ECodec::GzipNormal: {
            static TGzipCodec result(6);
            return &result;
        }

        case ECodec::GzipBestCompression: {
            static TGzipCodec result(9);
            return &result;
        }

        case ECodec::Lz4: {
            static TLz4Codec result(false);
            return &result;
        }

        case ECodec::Lz4HighCompression: {
            static TLz4Codec result(true);
            return &result;
        }

        case ECodec::QuickLz: {
            static TQuickLzCodec result;
            return &result;
        }

        default:
            YUNREACHABLE();
    }
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NCompression
} // namespace NYT

