#include "details.h"

#include <yt/core/misc/blob.h>
#include <yt/core/misc/finally.h>

#include <library/streams/brotli/brotli.h>

namespace NYT::NCompression {

////////////////////////////////////////////////////////////////////////////////

namespace {

class TBrotliStreamSourceIn
    : public IInputStream
{
public:
    explicit TBrotliStreamSourceIn(StreamSource* source)
        : Source_(source)
    { }

private:
    StreamSource* const Source_;

    size_t DoRead(void* buffer, size_t size) override
    {
        if (Source_->Available() == 0) {
            return 0;
        }

        size_t nRead;
        const char* ptr = Source_->Peek(&nRead);

        if (nRead > 0) {
            nRead = std::min(size, nRead);
            std::copy(ptr, ptr + nRead, static_cast<char*>(buffer));
            Source_->Skip(nRead);
        }

        return nRead;
    }
};

class TBrotliStreamSourceOut
    : public IOutputStream
{
public:
    explicit TBrotliStreamSourceOut(StreamSink* sink)
        : Sink_(sink)
    { }

private:
    StreamSink* const Sink_;

    virtual void DoWrite(const void *buf, size_t size) override
    {
        Sink_->Append(static_cast<const char*>(buf), size);
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

void BrotliCompress(int level, StreamSource* source, TBlob* output)
{
    ui64 totalInputSize = source->Available();
    output->Resize(sizeof(totalInputSize));

    // Write input size that will be used during decompression.
    TMemoryOutput memoryOutput(output->Begin(), sizeof(totalInputSize));
    WritePod(memoryOutput, totalInputSize);

    TDynamicByteArraySink sink(output);
    TBrotliStreamSourceOut sinkAdaptor(&sink);
    try {
        TBrotliCompress compress(&sinkAdaptor, level);
        while (source->Available() > 0) {
            size_t read;
            const char* ptr = source->Peek(&read);
            if (read > 0) {
                compress.Write(ptr, read);
                source->Skip(read);
            }
        }
    } catch (const std::exception&) {
        YCHECK(false && "Brotli compression failed");
    }
}

void BrotliDecompress(StreamSource* source, TBlob* output)
{
    ui64 outputSize;
    ReadPod(source, outputSize);
    output->Resize(outputSize);

    TBrotliStreamSourceIn sourceAdaptor(source);

    try {
        TBrotliDecompress decompress(&sourceAdaptor);
        ui64 remainingSize = outputSize;
        while (remainingSize > 0) {
            ui64 offset = outputSize - remainingSize;
            ui64 read = decompress.Read(output->Begin() + offset, remainingSize);
            if (read == 0) {
                break;
            }
            remainingSize -= read;
        }

        YCHECK(remainingSize == 0);
    } catch (const std::exception&) {
        YCHECK(false && "Brotli decompression failed");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCompression::NYT

