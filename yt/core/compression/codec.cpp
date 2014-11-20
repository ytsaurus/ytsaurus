#include "stdafx.h"
#include "codec.h"
#include "details.h"
#include "snappy.h"
#include "zlib.h"
#include "lz.h"

#include <core/tracing/trace_context.h>

namespace NYT {
namespace NCompression {

struct TCompressedBlockTag { };
struct TDecompressedBlockTag { };

////////////////////////////////////////////////////////////////////////////////

class TCodecBase
    : public ICodec
{
protected:
    static int ZeroSizeEstimator(const std::vector<int>&)
    {
        return 0;
    }

    template <class TBlockTag>
    TSharedRef Run(
        TConverter converter,
        // TODO(ignat): change bool to enum
        bool compress,
        const TSharedRef& ref)
    {
        auto guard = CreateTraceContextGuard(compress);

        ByteArraySource input(ref.Begin(), ref.Size());
        TRACE_ANNOTATION("input_size", ref.Size());

        auto output = TBlob(TBlockTag());
        converter(&input, &output);
        TRACE_ANNOTATION("output_size", output.Size());

        return TSharedRef::FromBlob(std::move(output));
    }

    template <class TBlockTag>
    TSharedRef Run(
        TConverter converter,
        bool compress,
        const std::vector<TSharedRef>& refs,
        std::function<int(const std::vector<int>&)> outputSizeEstimator = ZeroSizeEstimator)
    {
        auto guard = CreateTraceContextGuard(compress);

        if (refs.size() == 1) {
            return Run<TBlockTag>(
                converter,
                compress,
                refs.front());
        }

        std::vector<int> inputSizes;
        i64 totalInputSize = 0;
        for (const auto& ref : refs) {
            inputSizes.push_back(ref.Size());
            totalInputSize += ref.Size();
        }
        TRACE_ANNOTATION("input_size", totalInputSize);

        auto output = TBlob(TBlockTag());
        output.Reserve(outputSizeEstimator(inputSizes));

        TVectorRefsSource input(refs);
        converter(&input, &output);
        TRACE_ANNOTATION("output_size", output.Size());

        return TSharedRef::FromBlob(std::move(output));
    }

private:
    static NTracing::TChildTraceContextGuard CreateTraceContextGuard(bool compress)
    {
        return NTracing::TChildTraceContextGuard(
            "Compression",
            compress ? "Compress" : "Decompress");
    }
};

////////////////////////////////////////////////////////////////////////////////

class TNoneCodec
    : public TCodecBase
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
    : public TCodecBase
{
public:
    virtual TSharedRef Compress(const TSharedRef& block) override
    {
        return Run<TCompressedBlockTag>(NCompression::SnappyCompress, true, block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) override
    {
        return Run<TCompressedBlockTag>(NCompression::SnappyCompress, true, blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) override
    {
        return Run<TDecompressedBlockTag>(NCompression::SnappyDecompress, false, block);
    }

    virtual ECodec GetId() const override
    {
        return ECodec::Snappy;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TGzipCodec
    : public TCodecBase
{
public:
    explicit TGzipCodec(int level)
        : Compressor_(std::bind(NCompression::ZlibCompress, level, std::placeholders::_1, std::placeholders::_2))
        , Level_(level)
    { }

    virtual TSharedRef Compress(const TSharedRef& block) override
    {
        return Run<TCompressedBlockTag>(Compressor_, true, block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) override
    {
        return Run<TCompressedBlockTag>(Compressor_, false, blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) override
    {
        return Run<TDecompressedBlockTag>(NCompression::ZlibDecompress, false, block);
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
    : public TCodecBase
{
public:
    explicit TLz4Codec(bool highCompression)
        : Compressor_(std::bind(NCompression::Lz4Compress, highCompression, std::placeholders::_1, std::placeholders::_2))
        , CodecId_(highCompression ? ECodec::Lz4HighCompression : ECodec::Lz4)
    { }

    virtual TSharedRef Compress(const TSharedRef& block) override
    {
        return Run<TCompressedBlockTag>(
            Compressor_,
            true,
            block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) override
    {
        return Run<TCompressedBlockTag>(Compressor_, true, blocks, Lz4CompressionBound);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) override
    {
        return Run<TDecompressedBlockTag>(NCompression::Lz4Decompress, false, block);
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
    : public TCodecBase
{
public:
    explicit TQuickLzCodec()
        : Compressor_(NCompression::QuickLzCompress)
    { }

    virtual TSharedRef Compress(const TSharedRef& block) override
    {
        return Run<TCompressedBlockTag>(
            Compressor_,
            true,
            block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) override
    {
        return Run<TCompressedBlockTag>(
            Compressor_,
            true,
            blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) override
    {
        return Run<TDecompressedBlockTag>(NCompression::QuickLzDecompress, false, block);
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

