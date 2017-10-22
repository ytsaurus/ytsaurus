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
    { }

    ~TImpl()
    {
        if (Initialized_) {
            BrotliStateCleanup(&State_);
        }
    }

    size_t DoRead(void* buffer, size_t length)
    {
        YCHECK(length > 0);

        size_t availableOut = length;
        do {
            if (InputSize_ == 0 && !Exhausted_) {
                InputBuffer_ = TmpBuf();
                InputSize_ = Underlying_->Read((void*)InputBuffer_, TmpBufLen());

                if (InputSize_ == 0) {
                    Exhausted_ = true;
                }
            }

            if (!Initialized_) {
                BrotliStateInit(&State_);
                Initialized_ = true;
            }

            size_t bytesWritten = 0;
            auto result = BrotliDecompressBufferStreaming(
                &InputSize_,
                &InputBuffer_,
                0,
                &availableOut,
                (uint8_t**)&buffer,
                &bytesWritten,
                &State_);

            if (!Trusted_ && result == BROTLI_RESULT_ERROR) {
                BrotliStateCleanup(&State_);
                THROW_ERROR_EXCEPTION("Brotli decompression failed");
            }

            YCHECK(result != BROTLI_RESULT_ERROR);
            if (result == BROTLI_RESULT_SUCCESS) {
                BrotliStateCleanup(&State_);
                Initialized_ = false;
                break;
            }

            if (result == BROTLI_RESULT_NEEDS_MORE_OUTPUT) {
                break;
            }

            YCHECK(result == BROTLI_RESULT_NEEDS_MORE_INPUT);
            YCHECK(InputSize_ == 0);
        } while (length == availableOut && (InputSize_ != 0 || !Exhausted_));

        return length - availableOut;
    }

private:
    IInputStream* Underlying_;
    bool Trusted_;
    BrotliState State_;

    bool Initialized_ = false;
    bool Exhausted_ = false;
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

};

TBrotliDecompress::TBrotliDecompress(IInputStream* underlying, size_t buflen, bool trusted)
    : Impl_(new (buflen) TImpl(underlying, trusted))
{ }

size_t TBrotliDecompress::DoRead(void* buffer, size_t length)
{
    return Impl_->DoRead(buffer, length);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NCompression

