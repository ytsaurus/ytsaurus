#include "brotli_stream.h"
#include "public.h"

#include <yt/core/misc/error.h>

#include <util/memory/addstorage.h>

#include <contrib/libs/brotli/enc/encode.h>
#include <contrib/libs/brotli/dec/decode.h>

namespace NYT {
namespace NCompression {

////////////////////////////////////////////////////////////////////////////////

class TBrotliCompress::TImpl
{
public:
    inline TImpl(IOutputStream* underlying, int level)
        : Underlying_(underlying)
        , Compressor_(MakeParams(level))
    { }

    void DoWrite(const void* buffer, size_t length)
    {
        size_t consumed = 0;
        while (consumed < length) {
            const uint8_t* data = (const uint8_t*)buffer + consumed;
            auto bytesRead = std::min(length - consumed, Compressor_.input_block_size());
            Compressor_.CopyInputToRingBuffer(bytesRead, data);
            consumed += bytesRead;

            size_t outSize = 0;
            uint8_t* output = nullptr;
            YCHECK(Compressor_.WriteBrotliData(
                /* is_last = */ false,
                /* force_flush = */ false,
                &outSize,
                &output));

            if (outSize) {
                Underlying_->Write(output, outSize);
            }
        }
    }

    void DoFinish()
    {
        size_t outSize = 0;
        uint8_t* output = nullptr;
        YCHECK(Compressor_.WriteBrotliData(
            /* is_last = */ true,
            /* force_flush = */ true,
            &outSize,
            &output));
        Underlying_->Write(output, outSize);
    }

private:
    IOutputStream* Underlying_;
    brotli::BrotliCompressor Compressor_;

    static brotli::BrotliParams MakeParams(int level)
    {
        brotli::BrotliParams params;
        params.quality = level;
        return params;
    }
};

TBrotliCompress::TBrotliCompress(IOutputStream* underlying, int level)
    : Impl_(new TImpl(underlying, level))
{ }

TBrotliCompress::~TBrotliCompress() = default;

void TBrotliCompress::DoWrite(const void* buffer, size_t length)
{
    return Impl_->DoWrite(buffer, length);
}

void TBrotliCompress::DoFinish()
{
    return Impl_->DoFinish();
}

////////////////////////////////////////////////////////////////////////////////

class TBrotliDecompress::TImpl
    : public TAdditionalStorage<TImpl>
{
public:
    inline TImpl(IInputStream* underlying, bool trusted)
        : Underlying_(underlying)
        , Trusted_(trusted)
    {
        BrotliStateInit(&State_);
    }

    ~TImpl()
    {
        try {
            BrotliStateCleanup(&State_);
        } catch (...) {
        }
    }

    size_t DoRead(void* buffer, size_t length)
    {
        YCHECK(length > 0);

        size_t availableOut = length;
        BrotliResult result;
        do {
            if (InputSize_ == 0 && !InputExhausted_) {
                InputBuffer_ = TmpBuf();
                InputSize_ = Underlying_->Read((void*)InputBuffer_, TmpBufLen());

                if (InputSize_ == 0) {
                    InputExhausted_ = true;
                }
            }

            if (SubstreamFinished_ && !InputExhausted_) {
                ResetBrotliState();
            }

            size_t bytesWritten = 0;
            result = BrotliDecompressBufferStreaming(
                &InputSize_,
                &InputBuffer_,
                InputExhausted_,
                &availableOut,
                (uint8_t**)&buffer,
                &bytesWritten,
                &State_);
            SubstreamFinished_ = (result == BROTLI_RESULT_SUCCESS);
            if (!Trusted_ && result == BROTLI_RESULT_ERROR) {
                THROW_ERROR_EXCEPTION("Brotli decompression failed");
            }
            YCHECK(result != BROTLI_RESULT_ERROR);
        } while (result == BROTLI_RESULT_NEEDS_MORE_INPUT && !InputExhausted_);

        return length - availableOut;
    }

private:
    IInputStream* Underlying_;
    bool Trusted_;
    BrotliState State_;

    bool SubstreamFinished_ = false;
    bool InputExhausted_ = false;
    const uint8_t* InputBuffer_ = nullptr;
    size_t InputSize_ = 0;

    unsigned char* TmpBuf() noexcept
    {
        return (unsigned char*)AdditionalData();
    }

    size_t TmpBufLen() const noexcept
    {
        return AdditionalDataLength();
    }

    void ResetBrotliState()
    {
        BrotliStateCleanup(&State_);
        BrotliStateInit(&State_);
    }
};

TBrotliDecompress::TBrotliDecompress(IInputStream* underlying, size_t buflen, bool trusted)
    : Impl_(new (buflen) TImpl(underlying, trusted))
{ }

TBrotliDecompress::~TBrotliDecompress() = default;

size_t TBrotliDecompress::DoRead(void* buffer, size_t length)
{
    return Impl_->DoRead(buffer, length);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NCompression

