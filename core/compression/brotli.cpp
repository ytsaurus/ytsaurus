#include "details.h"

#include <yt/core/misc/blob.h>
#include <yt/core/misc/finally.h>

#include <contrib/libs/brotli/enc/encode.h>
#include <contrib/libs/brotli/dec/decode.h>

namespace NYT {
namespace NCompression {

////////////////////////////////////////////////////////////////////////////////

namespace {

class TBrotliStreamSourceIn
    : public brotli::BrotliIn
{
public:
    TBrotliStreamSourceIn(StreamSource* source)
        : Source_(source)
    { }

    const void* Read(size_t n, size_t* nread) override
    {
        const void* ptr = Source_->Peek(nread);
        if (*nread == 0) {
            return nullptr;
        }
        *nread = std::min(*nread, n);
        Source_->Skip(*nread);
        return ptr;
    }

private:
    StreamSource* const Source_;
};

class TBrotliStreamSourceOut
    : public brotli::BrotliOut
{
public:
    explicit TBrotliStreamSourceOut(StreamSink* sink)
        : Sink_(sink)
    { }

    virtual bool Write(const void *buf, size_t n) override
    {
        Sink_->Append(static_cast<const char*>(buf), n);
        return true;
    }

private:
    StreamSink* const Sink_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

void BrotliCompress(int level, StreamSource* source, TBlob* output)
{
    brotli::BrotliParams brotliParams;
    brotliParams.quality = level;

    ui64 totalInputSize = source->Available();
    output->Resize(sizeof(totalInputSize));

    ui64 curOutputPos = 0;
    // Write input size that will be used during decompression.
    {
        TMemoryOutput memoryOutput(output->Begin(), sizeof(totalInputSize));
        WritePod(memoryOutput, totalInputSize);
        curOutputPos += sizeof(totalInputSize);
    }

    TBrotliStreamSourceIn sourceAdaptor(source);
    TDynamicByteArraySink sink(output);
    TBrotliStreamSourceOut sinkAdaptor(&sink);
    YCHECK(brotli::BrotliCompress(brotliParams, &sourceAdaptor, &sinkAdaptor));
}

void BrotliDecompress(StreamSource* source, TBlob* output)
{
    ui64 outputSize;
    ReadPod(source, outputSize);
    output->Resize(outputSize);

    BrotliState state;
    BrotliStateInit(&state);
    auto brotliCleanupGuard = Finally([&] {
        BrotliStateCleanup(&state);
    });
    size_t consumedOutputSize = 0;
    bool isLastBlock = false;

    const char* inputPtr = nullptr;
    size_t availableInputSize = 0;
    auto* outputPtr = output->Begin();
    size_t availableOutputSize = outputSize;

    while (source->Available() > 0) {
        if (availableInputSize == 0) {
            inputPtr = source->Peek(&availableInputSize);
            if (availableInputSize == source->Available()) {
                isLastBlock = true;
            }
        }

        ui64 inputSizeBeforeDecompress = availableInputSize;
        auto result = BrotliDecompressBufferStreaming(
            &availableInputSize,
            reinterpret_cast<const uint8_t**>(&inputPtr),
            isLastBlock,
            &availableOutputSize,
            reinterpret_cast<uint8_t**>(&outputPtr),
            &consumedOutputSize,
            &state);

        if (!isLastBlock) {
            YCHECK(result == BROTLI_RESULT_NEEDS_MORE_INPUT);
        } else {
            YCHECK(result == BROTLI_RESULT_SUCCESS);
        }

        source->Skip(inputSizeBeforeDecompress - availableInputSize);
    }

    YCHECK(consumedOutputSize == outputSize);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NCompression

