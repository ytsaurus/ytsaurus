#include "lz.h"

#include <contrib/z-lz-lzo/lz4.h>
#include <contrib/z-lz-lzo/lz4hc.h>
#include <contrib/z-lz-lzo/quicklz.h>

namespace NYT {
namespace NCompression {

////////////////////////////////////////////////////////////////////////////////

namespace {

struct THeader
{
    i32 OutputSize;
    i32 InputSize;
};

} // anonymous namespace

void Lz4Compress(bool highCompression, StreamSource* source, TBlob* output)
{
    size_t currentPos = 0;
    while (source->Available() > 0) {
        size_t len;
        const char* input = source->Peek(&len);

        // LZ4 only supports i32 length.
        YCHECK(len <= 1 << 30);

        size_t bound =
            currentPos +
            sizeof(THeader) +
            LZ4_compressBound(len);
        output->Resize(bound, false);

        size_t headerPos = currentPos;
        currentPos += sizeof(THeader);

        THeader header;
        header.InputSize = len;
        if (highCompression) {
            header.OutputSize = LZ4_compressHC(input, output->Begin() + currentPos, len);
        } else {
            header.OutputSize = LZ4_compress(input, output->Begin() + currentPos, len);
        }
        YCHECK(header.OutputSize >= 0);

        currentPos += header.OutputSize;
        output->Resize(currentPos);

        TMemoryOutput memoryOutput(output->Begin() + headerPos, sizeof(THeader));
        WritePod(memoryOutput, header);

        source->Skip(len);
    }
}

void Lz4Decompress(StreamSource* source, TBlob* output)
{
    while (source->Available() > 0) {
        THeader header;
        ReadPod(source, header);

        size_t outputPos = output->Size();
        size_t newSize = outputPos + header.InputSize;
        output->Resize(newSize, false);

        TBlob input(header.OutputSize, false);
        Read(source, input.Begin(), input.Size());

        YCHECK(LZ4_uncompress(input.Begin(), output->Begin() + outputPos, header.InputSize) >= 0);
    }
}

////////////////////////////////////////////////////////////////////////////////

void QuickLzCompress(StreamSource* source, TBlob* output)
{
    size_t currentPos = 0;
    while (source->Available() > 0) {
        qlz_state_compress state;

        size_t len;
        const char* input = source->Peek(&len);

        size_t bound =
            currentPos +
            sizeof(THeader) +
            /* compressed bound */(len + 400);
        output->Resize(bound, false);

        size_t headerPos = currentPos;
        currentPos += sizeof(THeader);

        THeader header;
        header.InputSize = len;
        header.OutputSize = qlz_compress(input, output->Begin() + currentPos, len, &state);
        YCHECK(header.OutputSize >= 0);

        currentPos += header.OutputSize;
        output->Resize(currentPos);

        TMemoryOutput memoryOutput(output->Begin() + headerPos, sizeof(THeader));
        WritePod(memoryOutput, header);

        source->Skip(len);
    }
}

void QuickLzDecompress(StreamSource* source, TBlob* output)
{
    while (source->Available() > 0) {
        qlz_state_decompress state;

        THeader header;
        ReadPod(source, header);

        size_t outputPos = output->Size();
        size_t newSize = outputPos + header.InputSize;
        output->Resize(newSize, false);

        TBlob input(header.OutputSize, false);
        Read(source, input.Begin(), input.Size());

        YCHECK(qlz_decompress(input.Begin(), output->Begin() + outputPos, &state) >= 0);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCompression
} // namespace NYT

