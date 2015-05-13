#include "details.h"

#include <contrib/libs/zstd/lib/zstd_static.h>

namespace NYT {
namespace NCompression {

////////////////////////////////////////////////////////////////////////////////

const auto& Logger = CompressionLogger;

////////////////////////////////////////////////////////////////////////////////

void ZstdCompress(StreamSource* source, TBlob* output)
{
    size_t totalInputSize = source->Available();
    size_t maxOutputSize = ZSTD_compressBound(totalInputSize);
    output->Resize(maxOutputSize + sizeof(totalInputSize));
    size_t curOutputPos = 0;

    // Write input size that will be used during decompression.
    {
        TMemoryOutput memoryOutput(output->Begin(), sizeof(totalInputSize));
        WritePod(memoryOutput, totalInputSize);
        curOutputPos += sizeof(totalInputSize);
    }

    ZSTD_cctx_t context = ZSTD_createCCtx();

    // Write header.
    {
        auto outputPtr = reinterpret_cast<void*>(output->Begin() + curOutputPos);
        size_t compressedSize = ZSTD_compressBegin(
            context, outputPtr, maxOutputSize - curOutputPos);

        LOG_FATAL_IF(
            ZSTD_isError(compressedSize),
            ZSTD_getErrorName(compressedSize));

        curOutputPos += compressedSize;
    }

    while (source->Available()) {
        size_t inputSize;
        auto inputPtr = reinterpret_cast<const void*>(source->Peek(&inputSize));
        auto outputPtr = reinterpret_cast<void*>(output->Begin() + curOutputPos);

        size_t compressedSize = ZSTD_compressContinue(
            context, outputPtr, maxOutputSize - curOutputPos, inputPtr, inputSize);

        LOG_FATAL_IF(
            ZSTD_isError(compressedSize),
            ZSTD_getErrorName(compressedSize));

        curOutputPos += compressedSize;
        source->Skip(inputSize);
    }

    // Write footer.
    {
        auto outputPtr = reinterpret_cast<void*>(output->Begin() + curOutputPos);
        size_t compressedSize = ZSTD_compressEnd(context, outputPtr, maxOutputSize - curOutputPos);

        LOG_FATAL_IF(
            ZSTD_isError(compressedSize),
            ZSTD_getErrorName(compressedSize));

        curOutputPos += compressedSize;
    }

    output->Resize(curOutputPos);
}

void ZstdDecompress(StreamSource* source, TBlob* output)
{
    size_t outputSize;
    ReadPod(source, outputSize);
    output->Resize(outputSize);
    auto outputPtr = reinterpret_cast<void*>(output->Begin());

    size_t inputSize;
    auto inputPtr = reinterpret_cast<const void*>(source->Peek(&inputSize));

    size_t decompressedSize = ZSTD_decompress(outputPtr, outputSize, inputPtr, inputSize);

    // ZSTD_decompress returns error code instead of decompressed size if it fails.
    LOG_FATAL_IF(
        ZSTD_isError(decompressedSize),
        ZSTD_getErrorName(decompressedSize));

    YCHECK(decompressedSize == outputSize);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NCompression

