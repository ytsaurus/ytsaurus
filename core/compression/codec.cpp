#include "codec.h"
#include "bzip2.h"
#include "details.h"
#include "lz.h"
#include "lzma.h"
#include "snappy.h"
#include "zlib.h"
#include "zstd.h"
#include "zstd_legacy.h"
#include "brotli.h"

namespace NYT {
namespace NCompression {

////////////////////////////////////////////////////////////////////////////////

template <class TCodec>
struct TCompressedBlockTag { };

template <class TCodec>
struct TDecompressedBlockTag { };

////////////////////////////////////////////////////////////////////////////////

template <class TCodec>
class TCodecBase
    : public ICodec
{
public:
    virtual TSharedRef Compress(const TSharedRef& block) override
    {
        return Run(&TCodec::DoCompress, GetRefCountedTypeCookie<TCompressedBlockTag<TCodec>>(), block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) override
    {
        return Run(&TCodec::DoCompress, GetRefCountedTypeCookie<TCompressedBlockTag<TCodec>>(), blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) override
    {
        return Run(&TCodec::DoDecompress, GetRefCountedTypeCookie<TDecompressedBlockTag<TCodec>>(), block);
    }

    virtual TSharedRef Decompress(const std::vector<TSharedRef>& blocks) override
    {
        return Run(&TCodec::DoDecompress, GetRefCountedTypeCookie<TDecompressedBlockTag<TCodec>>(), blocks);
    }

private:
    TSharedRef Run(
        void (TCodec::*converter)(StreamSource* source, TBlob* output),
        TRefCountedTypeCookie blobCookie,
        const TSharedRef& ref)
    {
        ByteArraySource input(ref.Begin(), ref.Size());
        auto outputBlob = TBlob(blobCookie, 0, false);
        (static_cast<TCodec*>(this)->*converter)(&input, &outputBlob);
        return FinalizeBlob(&outputBlob, blobCookie);
    }

    TSharedRef Run(
        void (TCodec::*converter)(StreamSource* source, TBlob* output),
        TRefCountedTypeCookie blobCookie,
        const std::vector<TSharedRef>& refs)
    {
        if (refs.size() == 1) {
            return Run(converter, blobCookie, refs.front());
        }

        TVectorRefsSource input(refs);
        auto outputBlob = TBlob(blobCookie, 0, false);
        (static_cast<TCodec*>(this)->*converter)(&input, &outputBlob);
        return FinalizeBlob(&outputBlob, blobCookie);
    }

    static TSharedRef FinalizeBlob(TBlob* blob, TRefCountedTypeCookie blobCookie)
    {
        // For blobs smaller than 16K, do nothing.
        // For others, allow up to 5% capacity overhead.
        if (blob->Capacity() >= 16 * 1024 &&
            blob->Capacity() >= 1.05 * blob->Size())
        {
            *blob = TBlob(blobCookie, blob->Begin(), blob->Size());
        }
        return TSharedRef::FromBlob(std::move(*blob));
    }
};

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
        return MergeRefsToRef<TCompressedBlockTag<TNoneCodec>>(blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) override
    {
        return block;
    }

    virtual TSharedRef Decompress(const std::vector<TSharedRef>& blocks) override
    {
        return MergeRefsToRef<TDecompressedBlockTag<TNoneCodec>>(blocks);
    }

    virtual ECodec GetId() const override
    {
        return ECodec::None;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSnappyCodec
    : public TCodecBase<TSnappyCodec>
{
public:
    void DoCompress(StreamSource* source, TBlob* output)
    {
        NCompression::SnappyCompress(source, output);
    }

    void DoDecompress(StreamSource* source, TBlob* output)
    {
        NCompression::SnappyDecompress(source, output);
    }

    virtual ECodec GetId() const override
    {
        return ECodec::Snappy;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TZlibCodec
    : public TCodecBase<TZlibCodec>
{
public:
    explicit TZlibCodec(int level)
        : Level_(level)
    { }

    void DoCompress(StreamSource* source, TBlob* output)
    {
        NCompression::ZlibCompress(Level_, source, output);
    }

    void DoDecompress(StreamSource* source, TBlob* output)
    {
        NCompression::ZlibDecompress(source, output);
    }

    virtual ECodec GetId() const override
    {
        switch (Level_) {

#define CASE(level) case level: return PP_CONCAT(ECodec::Zlib_, level);
            PP_FOR_EACH(CASE, (1)(2)(3)(4)(5)(6)(7)(8)(9))
#undef CASE

            default:
                Y_UNREACHABLE();
        }
    }

private:
    const int Level_;
};

////////////////////////////////////////////////////////////////////////////////

class TLz4Codec
    : public TCodecBase<TLz4Codec>
{
public:
    explicit TLz4Codec(bool highCompression)
        : HighCompression_(highCompression)
    { }

    void DoCompress(StreamSource* source, TBlob* output)
    {
        NCompression::Lz4Compress(HighCompression_, source, output);
    }

    void DoDecompress(StreamSource* source, TBlob* output)
    {
        NCompression::Lz4Decompress(source, output);
    }

    virtual ECodec GetId() const override
    {
        return HighCompression_ ? ECodec::Lz4HighCompression : ECodec::Lz4;
    }

private:
    const bool HighCompression_;
};

////////////////////////////////////////////////////////////////////////////////

class TQuickLzCodec
    : public TCodecBase<TQuickLzCodec>
{
public:
    TQuickLzCodec()
    { }

    void DoCompress(StreamSource* source, TBlob* output)
    {
        NCompression::QuickLzCompress(source, output);
    }

    void DoDecompress(StreamSource* source, TBlob* output)
    {
        NCompression::QuickLzDecompress(source, output);
    }

    virtual ECodec GetId() const override
    {
        return ECodec::QuickLz;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TZstdLegacyCodec
    : public TCodecBase<TZstdLegacyCodec>
{
public:
    void DoCompress(StreamSource* source, TBlob* output)
    {
        NCompression::ZstdLegacyCompress(source, output);
    }

    void DoDecompress(StreamSource* source, TBlob* output)
    {
        NCompression::ZstdLegacyDecompress(source, output);
    }

    virtual ECodec GetId() const override
    {
        return ECodec::Zstd;
    }
};

class TZstdCodec
    : public TCodecBase<TZstdCodec>
{
public:
    TZstdCodec(int level)
        : Level_(level)
    { }

    void DoCompress(StreamSource* source, TBlob* output)
    {
        NCompression::ZstdCompress(Level_, source, output);
    }

    void DoDecompress(StreamSource* source, TBlob* output)
    {
        NCompression::ZstdDecompress(source, output);
    }

    virtual ECodec GetId() const override
    {
        switch (Level_) {

#define CASE(level) case level: return PP_CONCAT(ECodec::Zstd_, level);
            PP_FOR_EACH(CASE, (1)(2)(3)(4)(5)(6)(7)(8)(9)(10)(11)(12)(13)(14)(15)(16)(17)(18)(19)(20)(21))
#undef CASE

            default:
                Y_UNREACHABLE();
        }
    }

private:
    const int Level_;
};

////////////////////////////////////////////////////////////////////////////////

class TBrotliCodec
    : public TCodecBase<TBrotliCodec>
{
public:
    TBrotliCodec(int level)
        : Level_(level)
    { }

    void DoCompress(StreamSource* source, TBlob* output)
    {
        NCompression::BrotliCompress(Level_, source, output);
    }

    void DoDecompress(StreamSource* source, TBlob* output)
    {
        NCompression::BrotliDecompress(source, output);
    }

    virtual ECodec GetId() const override
    {
        switch (Level_) {

#define CASE(level) case level: return PP_CONCAT(ECodec::Brotli_, level);
            PP_FOR_EACH(CASE, (1)(2)(3)(4)(5)(6)(7)(8)(9)(10)(11))
#undef CASE

            default:
                Y_UNREACHABLE();
        }
    }

private:
    const int Level_;
};

class TLzmaCodec
    : public TCodecBase<TLzmaCodec>
{
public:
    TLzmaCodec(int level)
        : Level_(level)
    { }

    void DoCompress(StreamSource* source, TBlob* output)
    {
        NCompression::LzmaCompress(Level_, source, output);
    }

    void DoDecompress(StreamSource* source, TBlob* output)
    {
        NCompression::LzmaDecompress(source, output);
    }

    virtual ECodec GetId() const override
    {
        switch (Level_) {

#define CASE(level) case level: return PP_CONCAT(ECodec::Lzma_, level);
            PP_FOR_EACH(CASE, (0)(1)(2)(3)(4)(5)(6)(7)(8)(9))
#undef CASE

            default:
                Y_UNREACHABLE();
        }
    }

private:
    const int Level_;
};

class TBzip2Codec
    : public TCodecBase<TBzip2Codec>
{
public:
    TBzip2Codec(int level)
        : Level_(level)
    { }

    void DoCompress(StreamSource* source, TBlob* output)
    {
        NCompression::Bzip2Compress(Level_, source, output);
    }

    void DoDecompress(StreamSource* source, TBlob* output)
    {
        NCompression::Bzip2Decompress(source, output);
    }

    virtual ECodec GetId() const override
    {
        switch (Level_) {

#define CASE(level) case level: return PP_CONCAT(ECodec::Bzip2_, level);
            PP_FOR_EACH(CASE, (1)(2)(3)(4)(5)(6)(7)(8)(9))
#undef CASE

            default:
                Y_UNREACHABLE();
        }
    }

private:
    const int Level_;
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

        case ECodec::ZstdLegacy: {
            static TZstdLegacyCodec result;
            return &result;
        }


#define CASE(param)                                                 \
    case ECodec::PP_CONCAT(CODEC, PP_CONCAT(_, param)): {           \
        static PP_CONCAT(T, PP_CONCAT(CODEC, Codec)) result(param); \
        return &result;                                             \
    }

#define CODEC Zlib
        PP_FOR_EACH(CASE, (1)(2)(3)(4)(5)(6)(7)(8)(9))
#undef CODEC

#define CODEC Brotli
        PP_FOR_EACH(CASE, (1)(2)(3)(4)(5)(6)(7)(8)(9)(10)(11))
#undef CODEC

#define CODEC Lzma
        PP_FOR_EACH(CASE, (0)(1)(2)(3)(4)(5)(6)(7)(8)(9))
#undef CODEC

#define CODEC Bzip2
        PP_FOR_EACH(CASE, (1)(2)(3)(4)(5)(6)(7)(8)(9))
#undef CODEC

#define CODEC Zstd
        PP_FOR_EACH(CASE, (1)(2)(3)(4)(5)(6)(7)(8)(9)(10)(11)(12)(13)(14)(15)(16)(17)(18)(19)(20)(21))
#undef CODEC

#undef CASE

        default:
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCompression
} // namespace NYT

