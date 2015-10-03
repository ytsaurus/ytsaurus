#include "details.h"

#include <contrib/libs/brotli/enc/encode.h>
#include <contrib/libs/brotli/dec/decode.h>

#include <core/misc/blob.h>

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

size_t EstimateCompressedSize(size_t inputLength)
{
    // TODO(acid): Replace this with appropriate call to API when it is available.
    return inputLength * 1.2 + 10240;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void BrotliCompress(int level, StreamSource* source, TBlob* output)
{
    brotli::BrotliParams brotliParams;
    brotliParams.quality = level;

    ui64 totalInputSize = source->Available();
    size_t maxCompressedSize = EstimateCompressedSize(source->Available());
    output->Resize(sizeof(totalInputSize) + maxCompressedSize);

    ui64 curOutputPos = 0;
    // Write input size that will be used during decompression.
    {
        TMemoryOutput memoryOutput(output->Begin(), sizeof(totalInputSize));
        WritePod(memoryOutput, totalInputSize);
        curOutputPos += sizeof(totalInputSize);
    }

    TBrotliStreamSourceIn sourceAdaptor(source);
    brotli::BrotliMemOut outputAdaptor(output->Begin() + curOutputPos, maxCompressedSize);
    YCHECK(brotli::BrotliCompress(brotliParams, &sourceAdaptor, &outputAdaptor));

    size_t compressedSize = outputAdaptor.position();
    curOutputPos += compressedSize;
    output->Resize(curOutputPos);
}

void BrotliDecompress(StreamSource* source, TBlob* output)
{
    ui64 outputSize;
    ReadPod(source, outputSize);
    output->Resize(outputSize);

    BrotliState state;
    BrotliStateInit(&state);
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

