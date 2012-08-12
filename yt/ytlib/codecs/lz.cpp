#include "lz.h"

#include <contrib/z-lz-lzo/lz4.h>
#include <contrib/z-lz-lzo/lz4hc.h>
#include <contrib/z-lz-lzo/quicklz.h>

namespace NYT {
namespace NCodec {

////////////////////////////////////////////////////////////////////////////////

namespace {

struct THeader
{
    i32 OutputSize;
    i32 InputSize;
};

} // anonymous namespace

void Lz4Compress(bool highCompression, StreamSource* source, std::vector<char>* output)
{
    size_t currentPos = 0;
    while (source->Available() > 0) {
        size_t len;
        const char* input = source->Peek(&len);

        // LZ4 support only integer length
        YCHECK(len <= 1 << 30);

        size_t bound = currentPos + sizeof(THeader) + LZ4_compressBound(len);

        if (output->capacity() < bound) {
            output->reserve(std::max(bound, 2 * output->size()));
        }
        output->resize(bound);

        size_t headerPos = currentPos;
        currentPos += sizeof(THeader);

        THeader header;
        header.InputSize = len;
        if (highCompression) {
            header.OutputSize = LZ4_compressHC(input, output->data() + currentPos, len);
        }
        else {
            header.OutputSize = LZ4_compress(input, output->data() + currentPos, len);
        }
        YCHECK(header.OutputSize >= 0);

        currentPos += header.OutputSize;
        output->resize(currentPos);

        TMemoryOutput memoryOutput(output->data() + headerPos, sizeof(THeader));
        WritePod(memoryOutput, header);

        source->Skip(len);
    }
}

void Lz4Decompress(StreamSource* source, std::vector<char>* output)
{
    while (source->Available() > 0) {
        THeader header;
        ReadPod(source, header);

        size_t outputPos = output->size();
        size_t newSize = output->size() + header.InputSize;
        if (output->capacity() < newSize) {
            output->reserve(std::max(newSize, 2 * output->capacity()));
        }
        output->resize(newSize);

        std::vector<char> input(header.OutputSize);
        Read(source, input.data(), input.size());

        YCHECK(LZ4_uncompress(input.data(), output->data() + outputPos, header.InputSize) >= 0);
    }
}

////////////////////////////////////////////////////////////////////////////////

void QuickLzCompress(StreamSource* source, std::vector<char>* output)
{
    size_t currentPos = 0;
    while (source->Available() > 0) {
        qlz_state_compress state;

        size_t len;
        const char* input = source->Peek(&len);

        size_t bound = currentPos + sizeof(THeader) + 
            /* compressed bound */(len + 400);

        if (output->capacity() < bound) {
            output->reserve(std::max(bound, 2 * output->size()));
        }
        output->resize(bound);

        size_t headerPos = currentPos;
        currentPos += sizeof(THeader);

        THeader header;
        header.InputSize = len;
        header.OutputSize = qlz_compress(input, output->data() + currentPos, len, &state);
        YCHECK(header.OutputSize >= 0);

        currentPos += header.OutputSize;
        output->resize(currentPos);

        TMemoryOutput memoryOutput(output->data() + headerPos, sizeof(THeader));
        WritePod(memoryOutput, header);

        source->Skip(len);
    }
}

void QuickLzDecompress(StreamSource* source, std::vector<char>* output)
{
    while (source->Available() > 0) {
        qlz_state_decompress state;

        THeader header;
        ReadPod(source, header);

        size_t outputPos = output->size();
        size_t newSize = output->size() + header.InputSize;
        if (output->capacity() < newSize) {
            output->reserve(std::max(newSize, 2 * output->capacity()));
        }
        output->resize(newSize);

        std::vector<char> input(header.OutputSize);
        Read(source, input.data(), input.size());

        YCHECK(qlz_decompress(input.data(), output->data() + outputPos, &state) >= 0);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCodec

} // namespace NYT

