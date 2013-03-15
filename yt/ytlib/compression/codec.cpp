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
namespace NCompression {

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

    virtual ECodec GetId() const override
    {
        return ECodec::None;
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
        return NCompression::Apply(BIND(NCompression::SnappyCompress), block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) override
    {
        return NCompression::Apply(BIND(NCompression::SnappyCompress), blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) override
    {
        return NCompression::Apply(BIND(NCompression::SnappyDecompress), block);
    }

    virtual ECodec GetId() const override
    {
        return ECodec::Snappy;
    }

};

////////////////////////////////////////////////////////////////////////////////

class TGzipCodec
    : public TRefCounted
    , public ICodec
{
public:
    explicit TGzipCodec(int level)
        : Compressor_(BIND(NCompression::ZlibCompress, level))
        , Level_(level)
    { }

    virtual TSharedRef Compress(const TSharedRef& block) override
    {
        return NCompression::Apply(Compressor_, block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) override
    {
        return NCompression::Apply(Compressor_, blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) override
    {
        return NCompression::Apply(BIND(NCompression::ZlibDecompress), block);
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
    , public TRefCounted
{
public:
    explicit TLz4Codec(bool highCompression)
        : Compressor_(BIND(NCompression::Lz4Compress, highCompression))
        , CodecId_(highCompression ? ECodec::Lz4HighCompression : ECodec::Lz4)
    { }

    virtual TSharedRef Compress(const TSharedRef& block) override
    {
        return NCompression::Apply(Compressor_, block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) override
    {
        return NCompression::Apply(Compressor_, blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) override
    {
        return NCompression::Apply(BIND(NCompression::Lz4Decompress), block);
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
    : public TRefCounted
    , public ICodec
{
public:
    explicit TQuickLzCodec()
        : Compressor_(BIND(NCompression::QuickLzCompress))
    { }

    virtual TSharedRef Compress(const TSharedRef& block) override
    {
        return NCompression::Apply(Compressor_, block);
    }

    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) override
    {
        return NCompression::Apply(Compressor_, blocks);
    }

    virtual TSharedRef Decompress(const TSharedRef& block) override
    {
        return NCompression::Apply(BIND(NCompression::QuickLzDecompress), block);
    }

    virtual ECodec GetId() const override
    {
        return ECodec::QuickLz;
    }

private:
    NCompression::TConverter Compressor_;
};

ICodec* GetCodec(ECodec id)
{
    using namespace NCompression;

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
        case ECodec::None:
            return RefCountedSingleton<NCompression::TNoneCodec>().Get();

        case ECodec::Snappy:
            return RefCountedSingleton<NCompression::TSnappyCodec>().Get();

        case ECodec::GzipNormal:
            return GzipCodecNormal.Get();

        case ECodec::GzipBestCompression:
            return GzipCodecBestCompression.Get();

        case ECodec::Lz4:
            return Lz4.Get();

        case ECodec::Lz4HighCompression:
            return Lz4HighCompression.Get();

        case ECodec::QuickLz:
            return RefCountedSingleton<NCompression::TQuickLzCodec>().Get();

        default:
            YUNREACHABLE();
    }
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NCompression
} // namespace NYT

