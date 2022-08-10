#include "zstd.h"
#include "private.h"

#include <yt/yt/core/misc/blob.h>
#include <yt/yt/core/misc/finally.h>

#define ZSTD_STATIC_LINKING_ONLY
#include <contrib/libs/zstd/include/zstd.h>

namespace NYT::NCompression::NDetail {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CompressionLogger;
static constexpr size_t MaxZstdBlockSize = 1_MB;

////////////////////////////////////////////////////////////////////////////////

struct TZstdCompressBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

namespace {

size_t EstimateOutputSize(size_t totalInputSize)
{
    size_t headerSize = ZSTD_compressBound(0);
    size_t fullBlocksNumber = totalInputSize / MaxZstdBlockSize;
    size_t lastBlockSize = totalInputSize % MaxZstdBlockSize;
    return
        headerSize +
        fullBlocksNumber * ZSTD_compressBound(MaxZstdBlockSize) +
        ZSTD_compressBound(lastBlockSize);
}

} // namespace

void ZstdCompress(int level, TSource* source, TBlob* output)
{
    ui64 totalInputSize = source->Available();
    output->Resize(EstimateOutputSize(totalInputSize) + sizeof(totalInputSize), /*initializeStorage*/ false);
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

    auto checkError = [] (size_t result) {
        YT_LOG_FATAL_IF(ZSTD_isError(result), "Zstd compression failed (Error: %v)",
            ZSTD_getErrorName(result));
    };

    // Write header.
    {
        size_t headerSize = ZSTD_compressBegin(context, level);
        checkError(headerSize);
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
        checkError(compressedSize);
        curOutputPos += compressedSize;
    };

    TBlob block(TZstdCompressBufferTag(), MaxZstdBlockSize, false);
    size_t blockSize = 0;
    auto flushGuard = [&] {
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
            size_t takeSize = std::min(remainingSize, MaxZstdBlockSize - blockSize);
            fillBlock(takeSize);
            if (blockSize == MaxZstdBlockSize) {
                flushGuard();
            }
        }

        while (remainingSize >= MaxZstdBlockSize) {
            compressAndAppendBuffer(inputPtr, MaxZstdBlockSize);
            source->Skip(MaxZstdBlockSize);
            inputPtr = source->Peek(&inputSize);
            remainingSize -= MaxZstdBlockSize;
        }

        if (remainingSize > 0) {
            fillBlock(remainingSize);
        }

        YT_VERIFY(remainingSize == 0);
    }
    flushGuard();

    // Write footer.
    {
        void* outputPtr = output->Begin() + curOutputPos;
        size_t compressedSize = ZSTD_compressEnd(
            context,
            outputPtr,
            output->Size() - curOutputPos,
            nullptr,
            0);
        checkError(compressedSize);
        curOutputPos += compressedSize;
    }

    output->Resize(curOutputPos);
}

void ZstdDecompress(TSource* source, TBlob* output)
{
    ui64 outputSize;
    ReadPod(*source, outputSize);

    output->Resize(outputSize, /*initializeStorage*/ false);
    void* outputPtr = output->Begin();

    size_t inputSize;
    const void* inputPtr = source->Peek(&inputSize);

    // TODO(babenko): something weird is going on here.
    TBlob input;
    if (auto available = source->Available(); available > inputSize) {
        input.Resize(available, /*initializeStorage*/ false);
        ReadRef(*source, TMutableRef(input.Begin(), available));
        inputPtr = input.Begin();
        inputSize = input.Size();
    }

    size_t decompressedSize = ZSTD_decompress(outputPtr, outputSize, inputPtr, inputSize);
    if (ZSTD_isError(decompressedSize)) {
        THROW_ERROR_EXCEPTION("Zstd decompression failed: ZSTD_decompress returned an error")
            << TErrorAttribute("error", ZSTD_getErrorName(decompressedSize));
    }
    if (decompressedSize != outputSize) {
        THROW_ERROR_EXCEPTION("Zstd decompression failed: output size mismatch")
            << TErrorAttribute("expected_size", outputSize)
            << TErrorAttribute("actual_size", decompressedSize);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCompression::NDetail

