#include "compression.h"

#include <yt/core/ytree/serialize.h>

#include <library/streams/lzop/lzop.h>
#include <library/streams/lz/lz.h>
#include <library/streams/brotli/brotli.h>

#ifdef YT_IN_ARCADIA
#include <library/blockcodecs/codecs.h>
#include <library/blockcodecs/stream.h>
#endif

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
        TContentEncoding contentEncoding)
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
        Holder_ = buffer;

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
    const TContentEncoding ContentEncoding_;

    // NB: Arcadia streams got some "interesting" ideas about
    // exception handling and the role of destructors in the C++
    // programming language.
    TSharedRef Holder_;
    bool Destroying_ = false;
    std::unique_ptr<IOutputStream> Compressor_;

    void CreateCompressor()
    {
        if (Compressor_) {
            return;
        }

#ifdef YT_IN_ARCADIA
        if (ContentEncoding_.StartsWith("z-")) {
            Compressor_.reset(new NBlockCodecs::TCodedOutput(
                this,
                NBlockCodecs::Codec(ContentEncoding_.substr(2)),
                DefaultStreamBufferSize));
            return;
        }
#endif

        if (ContentEncoding_ == "gzip") {
            Compressor_.reset(new TZLibCompress(this, ZLib::GZip, 4, DefaultStreamBufferSize));
            return;
        }

        if (ContentEncoding_ == "deflate") {
            Compressor_.reset(new TZLibCompress(this, ZLib::ZLib, 4, DefaultStreamBufferSize));
            return;
        }

        if (ContentEncoding_ == "br") {
            Compressor_.reset(new TBrotliCompress(this, 3));
            return;
        }

        if (ContentEncoding_ == "x-lzop") {
            Compressor_.reset(new TLzopCompress(this, DefaultStreamBufferSize));
            return;
        }

        if (ContentEncoding_ == "y-lzo") {
            Compressor_.reset(new TLzoCompress(this, DefaultStreamBufferSize));
            return;
        }

        if (ContentEncoding_ == "y-lzf") {
            Compressor_.reset(new TLzfCompress(this, DefaultStreamBufferSize));
            return;
        }

        if (ContentEncoding_ == "y-snappy") {
            Compressor_.reset(new TSnappyCompress(this, DefaultStreamBufferSize));
            return;
        }
        
        THROW_ERROR_EXCEPTION("Unsupported content encoding")
            << TErrorAttribute("content_encoding", ToString(ContentEncoding_));
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

class TDecompressingInputStream
    : public IAsyncInputStream
    , private IInputStream
{
public:
    TDecompressingInputStream(
        IAsyncZeroCopyInputStreamPtr underlying,
        TContentEncoding contentEncoding)
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
    const TContentEncoding ContentEncoding_;
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

#ifdef YT_IN_ARCADIA
        if (ContentEncoding_.StartsWith("z-")) {
            Decompressor_.reset(new NBlockCodecs::TDecodedInput(
                this,
                NBlockCodecs::Codec(ContentEncoding_.substr(2))));
            return;
        }
#endif
        
        if (ContentEncoding_ == "gzip" || ContentEncoding_ == "deflate") {
            Decompressor_.reset(new TZLibDecompress(this));
            return;
        }

        if (ContentEncoding_ == "br") {
            Decompressor_.reset(new TBrotliDecompress(this));
            return;
        }

        if (ContentEncoding_ == "x-lzop") {
            Decompressor_.reset(new TLzopDecompress(this));
            return;
        }

        if (ContentEncoding_ == "y-lzo") {
            Decompressor_.reset(new TLzoDecompress(this));
            return;
        }

        if (ContentEncoding_ == "y-lzf") {
            Decompressor_.reset(new TLzfDecompress(this));
            return;
        }

        if (ContentEncoding_ == "y-snappy") {
            Decompressor_.reset(new TSnappyDecompress(this));
            return;
        }
        
        THROW_ERROR_EXCEPTION("Unsupported content encoding")
            << TErrorAttribute("content_encoding", ContentEncoding_);
    }
};

DEFINE_REFCOUNTED_TYPE(TDecompressingInputStream)

////////////////////////////////////////////////////////////////////////////////

TContentEncoding IdentityContentEncoding = "identity";

static const std::vector<TContentEncoding> SupportedCompressions = {
    "gzip",
    IdentityContentEncoding,
    "br",
    "x-lzop",
    "y-lzo",
    "y-lzf",
    "y-snappy",
    "deflate",
};

bool IsCompressionSupported(const TContentEncoding& contentEncoding)
{
#ifdef YT_IN_ARCADIA
    if (contentEncoding.StartsWith("z-")) {
        try {
            NBlockCodecs::Codec(contentEncoding.substr(2));
            return true;
        } catch (const NBlockCodecs::TNotFound& ) {
            return false;
        }
    }
#endif

    for (const auto& supported : SupportedCompressions) {
        if (supported == contentEncoding) {
            return true;
        }
    }
    
    return false;
}

// NOTE: Does not implement the spec, but a reasonable approximation.
TErrorOr<TContentEncoding> GetBestAcceptedEncoding(const TString& clientAcceptEncodingHeader)
{
    auto bestPosition = TString::npos;
    TContentEncoding bestEncoding;

    auto checkCandidate = [&] (const TString& candidate, size_t position) {
        if (position != TString::npos && (bestPosition == TString::npos || position < bestPosition)) {
            bestEncoding = candidate;
            bestPosition = position;
        }
    };
    
    for (const auto& candidate : SupportedCompressions) {
        if (candidate == "x-lzop") {
            continue;
        }

        auto position = clientAcceptEncodingHeader.find(candidate);
        checkCandidate(candidate, position);
    }

#ifdef YT_IN_ARCADIA
    for (const auto& blockcodec : NBlockCodecs::ListAllCodecs()) {
        auto candidate = TString{"z-"} + blockcodec;

        auto position = clientAcceptEncodingHeader.find(candidate);
        checkCandidate(candidate, position);
    }
#endif

    if (!bestEncoding.empty()) {
        return bestEncoding;
    }
    
    return TError("Could not determine feasible Content-Encoding given Accept-Encoding constraints")
        << TErrorAttribute("client_accept_encoding", clientAcceptEncodingHeader);
}

IAsyncOutputStreamPtr CreateCompressingAdapter(
    IAsyncOutputStreamPtr underlying,
    TContentEncoding contentEncoding)
{
    return New<TCompressingOutputStream>(underlying, contentEncoding);
}

IAsyncInputStreamPtr CreateDecompressingAdapter(
    IAsyncZeroCopyInputStreamPtr underlying,
    TContentEncoding contentEncoding)
{
    return New<TDecompressingInputStream>(underlying, contentEncoding);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttpProxy
} // namespace NYT
