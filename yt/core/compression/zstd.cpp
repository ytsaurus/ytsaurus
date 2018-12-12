#include "zstd.h"
#include "details.h"

#include <yt/core/misc/blob.h>
#include <yt/core/misc/finally.h>

#define ZSTD_STATIC_LINKING_ONLY
#include <contrib/libs/zstd/zstd.h>

namespace NYT::NCompression {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CompressionLogger;

static const size_t MaxBlockSize = 1_MB;

////////////////////////////////////////////////////////////////////////////////

struct TZstdCompressBufferTag {};

////////////////////////////////////////////////////////////////////////////////

static size_t EstimateOutputSize(size_t totalInputSize)
{
    size_t headerSize = ZSTD_compressBound(0);
    size_t fullBlocksNumber = totalInputSize / MaxBlockSize;
    size_t lastBlockSize = totalInputSize % MaxBlockSize;
    return headerSize +
           fullBlocksNumber * ZSTD_compressBound(MaxBlockSize) +
           ZSTD_compressBound(lastBlockSize);
}

void ZstdCompress(int level, StreamSource* source, TBlob* output)
{
    ui64 totalInputSize = source->Available();
    output->Resize(EstimateOutputSize(totalInputSize) + sizeof(totalInputSize));
    size_t curOutputPos = 0;

    // Write input size that will be used during decompression.
    {
        TMemoryOutput memoryOutput(output->Begin(), sizeof(totalInputSize));
        WritePod(memoryOutput, totalInputSize);
        curOutputPos += sizeof(totalInputSize);
    }

    auto context = ZSTD_createCCtx();
    auto contextGuard = Finally([&] () {
       ZSTD_freeCCtx(context);
    });

    // Write header.
    {
        size_t headerSize = ZSTD_compressBegin(context, level);

        YT_LOG_FATAL_IF(
            ZSTD_isError(headerSize),
            ZSTD_getErrorName(headerSize));

        curOutputPos += headerSize;
    }

    auto compressAndAppendBuffer = [&] (const void* buffer, size_t size) {
        if (size == 0) {
            return;
        }

        void* outputPtr = output->Begin() + curOutputPos;
        size_t compressedSize = ZSTD_compressContinue(
            context,
            outputPtr,
            output->Size() - curOutputPos,
            buffer,
            size);

        YT_LOG_FATAL_IF(
            ZSTD_isError(compressedSize),
            ZSTD_getErrorName(compressedSize));

        curOutputPos += compressedSize;
    };

    TBlob block(TZstdCompressBufferTag(), MaxBlockSize, false);
    size_t blockSize = 0;
    auto flushBlock = [&] () {
        compressAndAppendBuffer(block.Begin(), blockSize);
        blockSize = 0;
    };

    while (source->Available()) {
        size_t inputSize;
        const void* inputPtr = source->Peek(&inputSize);
        size_t remainingSize = inputSize;

        auto fillBlock = [&] (size_t size) {
            memcpy(block.Begin() + blockSize, inputPtr, size);
            blockSize += size;
            source->Skip(size);
            inputPtr = source->Peek(&inputSize);
            remainingSize -= size;
        };

        if (blockSize != 0) {
            size_t takeSize = std::min(remainingSize, MaxBlockSize - blockSize);
            fillBlock(takeSize);
            if (blockSize == MaxBlockSize) {
                flushBlock();
            }
        }

        while (remainingSize >= MaxBlockSize) {
            compressAndAppendBuffer(inputPtr, MaxBlockSize);
            source->Skip(MaxBlockSize);
            inputPtr = source->Peek(&inputSize);
            remainingSize -= MaxBlockSize;
        }

        if (remainingSize > 0) {
            fillBlock(remainingSize);
        }

        YCHECK(remainingSize == 0);
    }
    flushBlock();

    // Write footer.
    {
        void* outputPtr = output->Begin() + curOutputPos;
        size_t compressedSize = ZSTD_compressEnd(
            context,
            outputPtr,
            output->Size() - curOutputPos,
            nullptr,
            0);

        YT_LOG_FATAL_IF(
            ZSTD_isError(compressedSize),
            ZSTD_getErrorName(compressedSize));

        curOutputPos += compressedSize;
    }

    output->Resize(curOutputPos);
}

void ZstdDecompress(StreamSource* source, TBlob* output)
{
    ui64 outputSize;
    ReadPod(source, outputSize);
    output->Resize(outputSize);
    void* outputPtr = output->Begin();

    TBlob input;
    size_t inputSize;
    const void* inputPtr = source->Peek(&inputSize);

    if (source->Available() > inputSize) {
        Read(source, input);
        inputPtr = input.Begin();
        inputSize = input.Size();
    }

    size_t decompressedSize = ZSTD_decompress(outputPtr, outputSize, inputPtr, inputSize);

    // ZSTD_decompress returns error code instead of decompressed size if it fails.
    YT_LOG_FATAL_IF(
        ZSTD_isError(decompressedSize),
        ZSTD_getErrorName(decompressedSize));

    YCHECK(decompressedSize == outputSize);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCompression::NYT

