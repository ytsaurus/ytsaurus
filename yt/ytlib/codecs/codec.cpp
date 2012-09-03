#include "stdafx.h"
#include "codec.h"
#include "helpers.h"
#include "snappy.h"
#include "zlib.h"
#include "lz.h"

#include <ytlib/actions/bind.h>

#include <ytlib/misc/lazy_ptr.h>
#include <ytlib/misc/singleton.h>

#include <util/generic/singleton.h>

namespace NYT {
namespace NCodec {

////////////////////////////////////////////////////////////////////////////////

class TNoneCodec
    : public TRefCounted
    , public ICodec
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
};

////////////////////////////////////////////////////////////////////////////////

class TSnappyCodec
    : public TRefCounted
    , public ICodec
{
public:
    virtual TSharedRef Compress(const TSharedRef& block) override
    {
        return NCodec::Apply(BIND(NCodec::SnappyCompress), block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) override
    {
        return NCodec::Apply(BIND(NCodec::SnappyCompress), blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) override
    {
        return NCodec::Apply(BIND(NCodec::SnappyDecompress), block);
    }

};

////////////////////////////////////////////////////////////////////////////////

class TGzipCodec
    : public TRefCounted
    , public ICodec
{
public:
    explicit TGzipCodec(int level)
        : Compressor_(BIND(NCodec::ZlibCompress, level))
    { }

    virtual TSharedRef Compress(const TSharedRef& block) override
    {
        return NCodec::Apply(Compressor_, block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) override
    {
        return NCodec::Apply(Compressor_, blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) override
    {
        return NCodec::Apply(BIND(NCodec::ZlibDecompress), block);
    }

private:
    NCodec::TConverter Compressor_;
};

////////////////////////////////////////////////////////////////////////////////

class TLz4Codec
    : public ICodec
    , public TRefCounted
{
public:
    explicit TLz4Codec(bool highCompression)
        : Compressor_(BIND(NCodec::Lz4Compress, highCompression))
    { }

    virtual TSharedRef Compress(const TSharedRef& block) override
    {
        return NCodec::Apply(Compressor_, block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) override
    {
        return NCodec::Apply(Compressor_, blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) override
    {
        return NCodec::Apply(BIND(NCodec::Lz4Decompress), block);
    }

private:
    NCodec::TConverter Compressor_;
};

////////////////////////////////////////////////////////////////////////////////

class TQuickLzCodec
    : public TRefCounted
    , public ICodec
{
public:
    explicit TQuickLzCodec()
        : Compressor_(BIND(NCodec::QuickLzCompress))
    { }

    virtual TSharedRef Compress(const TSharedRef& block) override
    {
        return NCodec::Apply(Compressor_, block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) override
    {
        return NCodec::Apply(Compressor_, blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) override
    {
        return NCodec::Apply(BIND(NCodec::QuickLzDecompress), block);
    }

private:
    NCodec::TConverter Compressor_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCodec

ICodec* GetCodec(ECodecId id)
{
    using namespace NCodec;

    static TLazyPtr<TGzipCodec> GzipCodecNormal(
        BIND([] () {
            return New<TGzipCodec>(6);
        })
    );

    static TLazyPtr<TGzipCodec> GzipCodecBestCompression(
        BIND([] () {
            return New<TGzipCodec>(9);
        })
    );

    static TLazyPtr<TLz4Codec> Lz4(
        BIND([] () {
            return New<TLz4Codec>(false);
        })
    );

    static TLazyPtr<TLz4Codec> Lz4HighCompression(
        BIND([] () {
            return New<TLz4Codec>(true);
        })
    );

    switch (id) {
        case ECodecId::None:
            return RefCountedSingleton<NCodec::TNoneCodec>().Get();

        case ECodecId::Snappy:
            return RefCountedSingleton<NCodec::TSnappyCodec>().Get();

        case ECodecId::GzipNormal:
            return GzipCodecNormal.Get();

        case ECodecId::GzipBestCompression:
            return GzipCodecBestCompression.Get();

        case ECodecId::Lz4:
            return Lz4.Get();

        case ECodecId::Lz4HighCompression:
            return Lz4HighCompression.Get();

        case ECodecId::QuickLz:
            return RefCountedSingleton<NCodec::TQuickLzCodec>().Get();

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

