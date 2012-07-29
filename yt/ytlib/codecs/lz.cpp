#include "lz.h"

#include <contrib/z-lz-lzo/lz4.h>
//#include <contrib/z-lz-lzo/lz4hc.h>

namespace NYT {

namespace {

struct Header {
    i32 outputSize;
    i32 inputSize;
};

} // anonymous namespace

void Lz4Compress(StreamSource* source, std::vector<char>* output)
{
    size_t currentPos = 0;
    while (source->Available() > 0) {
        size_t len;
        const char* input = source->Peek(&len);

        // LZ4 support only integer length
        YCHECK(len <= 1 << 30);

        size_t bound = currentPos + sizeof(Header) + LZ4_compressBound(len);

        if (output->capacity() < bound) {
            output->reserve(2 * output->size());
        }
        output->resize(bound);

        size_t headerPos = currentPos;
        currentPos += sizeof(Header);

        Header header;
        header.inputSize = len;
        header.outputSize = LZ4_compress(input, output->data() + currentPos, len);

        currentPos += header.outputSize;
        output->resize(currentPos);

        TMemoryOutput memoryOutput(output->data() + headerPos, sizeof(Header));
        WritePod(memoryOutput, header);

        source->Skip(len);
    }
}

void Lz4Decompress(StreamSource* source, std::vector<char>* output)
{
    while (source->Available() > 0) {
        Header header;
        ReadPod(source, header);

        size_t outputPos = output->size();
        size_t newSize = output->size() + header.inputSize;
        if (output->capacity() < newSize) {
            output->reserve(2 * output->capacity());
        }
        output->resize(newSize);

        std::vector<char> input(header.outputSize);
        Read(source, input.data(), input.size());

        LZ4_uncompress(input.data(), output->data() + outputPos, header.inputSize);
    }
}

} // namespace NYT

