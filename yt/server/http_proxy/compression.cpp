#include "compression.h"

#include <yt/core/ytree/serialize.h>

#include <library/streams/lzop/lzop.h>
#include <library/streams/lz/lz.h>
#include <library/streams/brotli/brotli.h>

#include <util/stream/zlib.h>

namespace NYT {
namespace NHttpProxy {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

struct TStreamHolder
    : public TIntrinsicRefCounted
{
    explicit TStreamHolder(IAsyncOutputStreamPtr output)
        : Output(std::move(output))
    { }

    IAsyncOutputStreamPtr Output;
};

DEFINE_REFCOUNTED_TYPE(TStreamHolder)

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TSharedRefOutputStream::Write(const TSharedRef& buffer)
{
    Refs_.push_back(TSharedRef::MakeCopy<TDefaultSharedBlobTag>(buffer));
    return VoidFuture;
}

TFuture<void> TSharedRefOutputStream::Close()
{
    return VoidFuture;
}

const std::vector<TSharedRef>& TSharedRefOutputStream::GetRefs() const
{
    return Refs_;
}

////////////////////////////////////////////////////////////////////////////////

class TCompressingOutputStream
    : public IAsyncOutputStream
    , private IOutputStream
{
public:
    TCompressingOutputStream(
        IAsyncOutputStreamPtr underlying,
        EContentEncoding contentEncoding)
        : Underlying_(underlying)
        , ContentEncoding_(contentEncoding)
    { }

    ~TCompressingOutputStream()
    {
        Destroying_ = true;
        Compressor_.reset();
    }

    virtual TFuture<void> Write(const TSharedRef& buffer) override
    {
        CreateCompressor();
        Compressor_->Write(buffer.Begin(), buffer.Size());
        return VoidFuture;
    }

    virtual TFuture<void> Close() override
    {
        CreateCompressor();
        Compressor_->Finish();
        return VoidFuture;
    }

private:
    const IAsyncOutputStreamPtr Underlying_;
    const EContentEncoding ContentEncoding_;

    std::unique_ptr<IOutputStream> Compressor_;
    bool Destroying_ = false;

    void CreateCompressor()
    {
        if (Compressor_) {
            return;
        }

        switch (ContentEncoding_) {
            case EContentEncoding::Gzip:
                Compressor_.reset(new TZLibCompress(this, ZLib::GZip, 4, DefaultStreamBufferSize));
                return;
            case EContentEncoding::Deflate:
                Compressor_.reset(new TZLibCompress(this, ZLib::ZLib, 4, DefaultStreamBufferSize));
                return;
            case EContentEncoding::Lzop:
                Compressor_.reset(new TLzopCompress(this, DefaultStreamBufferSize));
                break;
            case EContentEncoding::Lzo:
                Compressor_.reset(new TLzoCompress(this, DefaultStreamBufferSize));
                break;
            case EContentEncoding::Lzf:
                Compressor_.reset(new TLzfCompress(this, DefaultStreamBufferSize));
                break;
            case EContentEncoding::Snappy:
                Compressor_.reset(new TSnappyCompress(this, DefaultStreamBufferSize));
                break;
            case EContentEncoding::Brotli:
                Compressor_.reset(new TBrotliCompress(this, 3));
                break;
            default:
                THROW_ERROR_EXCEPTION("Unsupported content encoding")
                    << TErrorAttribute("content_encoding", ToString(ContentEncoding_));
        }
    }
    
    virtual void DoWrite(const void* buf, size_t len) override
    {
        if (Destroying_) {
            return;
        }
    
        WaitFor(Underlying_->Write(TSharedRef(buf, len, New<TStreamHolder>(this))))
            .ThrowOnError();
    }

    virtual void DoFlush() override
    { }

    virtual void DoFinish() override
    {
        if (Destroying_) {
            return;
        }
    
        WaitFor(Underlying_->Close())
            .ThrowOnError();
    }
};

DEFINE_REFCOUNTED_TYPE(TCompressingOutputStream)

////////////////////////////////////////////////////////////////////////////////

class TCompressingInputStream
    : public IAsyncInputStream
    , private IInputStream
{
public:
    TCompressingInputStream(
        IAsyncZeroCopyInputStreamPtr underlying,
        EContentEncoding contentEncoding)
        : Underlying_(underlying)
        , ContentEncoding_(contentEncoding)
    { }

    virtual TFuture<size_t> Read(const TSharedMutableRef& buffer) override
    {
        CreateDecompressor();
        return MakeFuture<size_t>(Decompressor_->Read(buffer.Begin(), buffer.Size()));
    }

private:
    const IAsyncZeroCopyInputStreamPtr Underlying_;
    const EContentEncoding ContentEncoding_;
    std::unique_ptr<IInputStream> Decompressor_;

    TSharedRef LastRead_;
    bool IsEnd_ = false;

    virtual size_t DoRead(void* buf, size_t len) override
    {
        if (IsEnd_) {
            return 0;
        }

        if (LastRead_.Empty()) {
            LastRead_ = WaitFor(Underlying_->Read())
                .ValueOrThrow();
            IsEnd_ = LastRead_.Empty();
        }

        size_t readSize = std::min(len, LastRead_.Size());
        std::copy(LastRead_.Begin(), LastRead_.Begin() + readSize, reinterpret_cast<char*>(buf));
        if (readSize != LastRead_.Size()) {
            LastRead_ = LastRead_.Slice(readSize, LastRead_.Size());
        } else {
            LastRead_ = {};
        }
        return readSize;
    }

    void CreateDecompressor()
    {
        if (Decompressor_) {
            return;
        }

        switch (ContentEncoding_) {
            case EContentEncoding::Gzip:
            case EContentEncoding::Deflate:
                Decompressor_.reset(new TZLibDecompress(this));
                return;
            case EContentEncoding::Lzop:
                Decompressor_.reset(new TLzopDecompress(this));
                break;
            case EContentEncoding::Lzo:
                Decompressor_.reset(new TLzoDecompress(this));
                break;
            case EContentEncoding::Lzf:
                Decompressor_.reset(new TLzfDecompress(this));
                break;
            case EContentEncoding::Snappy:
                Decompressor_.reset(new TSnappyDecompress(this));
                break;
            case EContentEncoding::Brotli:
                Decompressor_.reset(new TBrotliDecompress(this));
                break;

            default:
                THROW_ERROR_EXCEPTION("Unsupported content encoding")
                    << TErrorAttribute("content_encoding", ToString(ContentEncoding_));
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TCompressingInputStream)

////////////////////////////////////////////////////////////////////////////////

IAsyncOutputStreamPtr CreateCompressingAdapter(
    IAsyncOutputStreamPtr underlying,
    EContentEncoding contentEncoding)
{
    return New<TCompressingOutputStream>(underlying, contentEncoding);
}

IAsyncInputStreamPtr CreateCompressingAdapter(
    IAsyncZeroCopyInputStreamPtr underlying,
    EContentEncoding contentEncoding)
{
    return New<TCompressingInputStream>(underlying, contentEncoding);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttpProxy
} // namespace NYT
