#include "brotli_stream.h"
#include "public.h"

#include <util/memory/addstorage.h>

#include <contrib/libs/brotli/enc/encode.h>
#include <contrib/libs/brotli/dec/decode.h>

namespace NYT {
namespace NCompression {

////////////////////////////////////////////////////////////////////////////////

class TBrotliCompress::TImpl
{
public:
    inline TImpl(IOutputStream* slave, int level)
        : Slave_(slave)
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
                Slave_->Write(output, outSize);
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
        Slave_->Write(output, outSize);
    }

private:
    IOutputStream* Slave_;
    brotli::BrotliCompressor Compressor_;

    static brotli::BrotliParams MakeParams(int level)
    {
        brotli::BrotliParams params;
        params.quality = level;
        return params;
    }
};

TBrotliCompress::TBrotliCompress(IOutputStream* slave, int level)
    : Impl_(new TImpl(slave, level))
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
    inline TImpl(IInputStream* slave)
        : Slave_(slave)
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
                InputSize_ = Slave_->Read((void*)InputBuffer_, TmpBufLen());

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
    IInputStream* Slave_;
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

TBrotliDecompress::TBrotliDecompress(IInputStream* slave, size_t buflen)
    : Impl_(new (buflen) TImpl(slave))
{ }

size_t TBrotliDecompress::DoRead(void* buffer, size_t length)
{
    return Impl_->DoRead(buffer, length);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NCompression

