#include "stdafx.h"
#include "codec.h"
#include "details.h"
#include "snappy.h"
#include "zlib.h"
#include "lz.h"
#include "zstd.h"

#include <core/tracing/trace_context.h>

namespace NYT {
namespace NCompression {

////////////////////////////////////////////////////////////////////////////////

template <class TCodec>
struct TCompressedBlockTag { };

template <class TCodec>
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

    template <class TCodec>
    TSharedRef Run(
        TConverter converter,
        // TODO(ignat): change bool to enum
        bool compress,
        const TSharedRef& ref)
    {
        // XXX(sandello): Disable tracing to due excessive output.
        // auto guard = CreateTraceContextGuard(compress);

        ByteArraySource input(ref.Begin(), ref.Size());
        // TRACE_ANNOTATION("input_size", ref.Size());

        auto blobCookie = compress
             ? GetRefCountedTypeCookie<TCompressedBlockTag<TCodec>>()
             : GetRefCountedTypeCookie<TDecompressedBlockTag<TCodec>>();
        auto outputBlob = TBlob(blobCookie, 0, false);
        converter(&input, &outputBlob);
        // TRACE_ANNOTATION("output_size", output.Size());

        return TSharedRef::FromBlob(std::move(outputBlob));
    }

    template <class TCodec>
    TSharedRef Run(
        TConverter converter,
        bool compress,
        const std::vector<TSharedRef>& refs,
        std::function<int(const std::vector<int>&)> outputSizeEstimator = ZeroSizeEstimator)
    {
        // XXX(sandello): Disable tracing to due excessive output.
        // auto guard = CreateTraceContextGuard(compress);

        if (refs.size() == 1) {
            return Run<TCodec>(
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
        // TRACE_ANNOTATION("input_size", totalInputSize);

        auto blobCookie = compress
              ? GetRefCountedTypeCookie<TCompressedBlockTag<TCodec>>()
              : GetRefCountedTypeCookie<TDecompressedBlockTag<TCodec>>();
        auto outputBlob = TBlob(blobCookie, 0, false);
        outputBlob.Reserve(outputSizeEstimator(inputSizes));

        TVectorRefsSource input(refs);
        converter(&input, &outputBlob);
        // TRACE_ANNOTATION("output_size", output.Size());

        return TSharedRef::FromBlob(std::move(outputBlob));
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

    virtual TSharedRef Decompress(const std::vector<TSharedRef>& blocks) override
    {
        return MergeRefs(blocks);
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
        return Run<TSnappyCodec>(NCompression::SnappyCompress, true, block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) override
    {
        return Run<TSnappyCodec>(NCompression::SnappyCompress, true, blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) override
    {
        return Run<TSnappyCodec>(NCompression::SnappyDecompress, false, block);
    }

    virtual TSharedRef Decompress(const std::vector<TSharedRef>& blocks) override
    {
        return Run<TSnappyCodec>(NCompression::SnappyDecompress, false, blocks);
    }

    virtual ECodec GetId() const override
    {
        return ECodec::Snappy;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TZlibCodec
    : public TCodecBase
{
public:
    explicit TZlibCodec(int level)
        : Compressor_(std::bind(NCompression::ZlibCompress, level, std::placeholders::_1, std::placeholders::_2))
        , Level_(level)
    { }

    virtual TSharedRef Compress(const TSharedRef& block) override
    {
        return Run<TZlibCodec>(Compressor_, true, block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) override
    {
        return Run<TZlibCodec>(Compressor_, true, blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) override
    {
        return Run<TZlibCodec>(NCompression::ZlibDecompress, false, block);
    }

    virtual TSharedRef Decompress(const std::vector<TSharedRef>& blocks) override
    {
        return Run<TZlibCodec>(NCompression::ZlibDecompress, false, blocks);
    }

    virtual ECodec GetId() const override
    {
        switch (Level_) {
            case 6:
                return ECodec::Zlib6;
            case 9:
                return ECodec::Zlib9;
            default:
                YUNREACHABLE();
        }
    }

private:
    const NCompression::TConverter Compressor_;
    const int Level_;
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
        return Run<TLz4Codec>(Compressor_, true, block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) override
    {
        return Run<TLz4Codec>(Compressor_, true, blocks, Lz4CompressionBound);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) override
    {
        return Run<TLz4Codec>(NCompression::Lz4Decompress, false, block);
    }

    virtual TSharedRef Decompress(const std::vector<TSharedRef>& blocks) override
    {
        return Run<TLz4Codec>(NCompression::Lz4Decompress, false, blocks);
    }

    virtual ECodec GetId() const override
    {
        return CodecId_;
    }

private:
    const NCompression::TConverter Compressor_;
    const ECodec CodecId_;
};

////////////////////////////////////////////////////////////////////////////////

class TQuickLzCodec
    : public TCodecBase
{
public:
    TQuickLzCodec()
        : Compressor_(NCompression::QuickLzCompress)
    { }

    virtual TSharedRef Compress(const TSharedRef& block) override
    {
        return Run<TQuickLzCodec>(Compressor_, true, block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) override
    {
        return Run<TQuickLzCodec>(Compressor_, true, blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) override
    {
        return Run<TQuickLzCodec>(NCompression::QuickLzDecompress, false, block);
    }

    virtual TSharedRef Decompress(const std::vector<TSharedRef>& blocks) override
    {
        return Run<TQuickLzCodec>(NCompression::QuickLzDecompress, false, blocks);
    }

    virtual ECodec GetId() const override
    {
        return ECodec::QuickLz;
    }

private:
    const NCompression::TConverter Compressor_;
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

        case ECodec::Zlib6: {
            static TZlibCodec result(6);
            return &result;
        }

        case ECodec::Zlib9: {
            static TZlibCodec result(9);
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

