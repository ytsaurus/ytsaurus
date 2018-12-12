#include "lzma.h"

extern "C" {
#include <contrib/libs/lzmasdk/LzmaEnc.h>
#include <contrib/libs/lzmasdk/LzmaDec.h>
}

namespace NYT::NCompression {

namespace {

const auto& Logger = CompressionLogger;

// ISzAlloc is an interface containing alloc/free functions (with its own signatures)
// required by lzma API.
class TSzAlloc
    : public ISzAlloc
{
public:
    TSzAlloc()
    {
        Alloc = &MallocWrap;
        Free = &FreeWrap;
    }

    static void* MallocWrap(const ISzAlloc*, size_t len)
    {
        return malloc(len);
    }

    static void FreeWrap(const ISzAlloc*, void* ptr)
    {
        free(ptr);
    }
};

TSzAlloc Alloc;

} // namespace


class TLzmaWriteWrapper
    : public ISeqOutStream
{
public:
    TLzmaWriteWrapper(TBlob* output)
        : Output_(output)
    {
        Write = &TLzmaWriteWrapper::WriteDataProc;
    }

    size_t WriteData(const void* buffer, size_t size)
    {
        Output_->Append(buffer, size);
        return size;
    }

    static size_t WriteDataProc(const ISeqOutStream* lzmaWriteWrapper, const void* buffer, size_t size)
    {
        TLzmaWriteWrapper* pThis = const_cast<TLzmaWriteWrapper*>(static_cast<const TLzmaWriteWrapper*>(lzmaWriteWrapper));
        return pThis->WriteData(buffer, size);
    }

private:
    TBlob* Output_;
};


class TLzmaReadWrapper
    : public ISeqInStream
{
public:
    TLzmaReadWrapper(StreamSource* source)
        : Source_(source)
    {
        Read = ReadDataProc;
    }

    SRes ReadData(void* buffer, size_t* size)
    {
        size_t peekedSize;
        const void* peekedData = Source_->Peek(&peekedSize);
        peekedSize = std::min(*size, peekedSize);
        peekedSize = std::min(peekedSize, Source_->Available());

        memcpy(buffer, peekedData, peekedSize);

        Source_->Skip(peekedSize);
        *size = peekedSize;
        return SZ_OK;
    }

    static SRes ReadDataProc(const ISeqInStream* lzmaReadWrapper, void* buffer, size_t* size)
    {
        TLzmaReadWrapper* pThis = const_cast<TLzmaReadWrapper*>(static_cast<const TLzmaReadWrapper*>(lzmaReadWrapper));
        return pThis->ReadData(buffer, size);
    }

private:
    StreamSource* const Source_;
};


void Check(SRes sres)
{
    YT_LOG_FATAL_IF(sres != SZ_OK, "Lzma failed with errorcode %v", sres);
}


void LzmaCompress(int level, StreamSource* source, TBlob* output)
{
    YCHECK(0 <= level && level <= 9);

    TLzmaReadWrapper reader(source);
    TLzmaWriteWrapper writer(output);

    CLzmaEncHandle handle = LzmaEnc_Create(&Alloc);
    YCHECK(handle);

    {
        // Set properties.
        CLzmaEncProps props;
        LzmaEncProps_Init(&props);
        props.level = level;
        props.writeEndMark = 1;

        Check(LzmaEnc_SetProps(handle, &props));
    }

    {
        // Write properties.
        Byte propsBuffer[LZMA_PROPS_SIZE];
        size_t propsBufferSize = LZMA_PROPS_SIZE;
        Check(LzmaEnc_WriteProperties(handle, propsBuffer, &propsBufferSize));
        writer.WriteData(propsBuffer, sizeof(propsBuffer));
    }

    // Compress data.
    Check(LzmaEnc_Encode(handle, &writer, &reader, nullptr, &Alloc, &Alloc));

    LzmaEnc_Destroy(handle, &Alloc, &Alloc);
}

void LzmaDecompress(StreamSource* source, TBlob* output)
{
    Byte propsBuffer[LZMA_PROPS_SIZE];
    Read(source, reinterpret_cast<char*>(propsBuffer), sizeof(propsBuffer));

    CLzmaDec handle;
    LzmaDec_Construct(&handle);
    Check(LzmaDec_Allocate(&handle, propsBuffer, LZMA_PROPS_SIZE, &Alloc));
    LzmaDec_Init(&handle);

    ELzmaStatus status = LZMA_STATUS_NOT_FINISHED;
    while (source->Available()) {
        size_t sourceDataSize;
        const Byte* sourceData = reinterpret_cast<const Byte*>(source->Peek(&sourceDataSize));
        sourceDataSize = std::min(sourceDataSize, source->Available());

        const size_t oldDicPos = handle.dicPos;

        SizeT bufferSize = sourceDataSize;
        Check(LzmaDec_DecodeToDic(
            &handle,
            handle.dicBufSize,
            sourceData,
            &bufferSize, // It's input buffer size before call, read byte count afterwards.
            LZMA_FINISH_ANY,
            &status));

        output->Append(handle.dic + oldDicPos, handle.dicPos - oldDicPos);

        sourceData += bufferSize;
        sourceDataSize -= bufferSize;

        // Strange lzma api requires us to update this index by hand.
        if (handle.dicPos == handle.dicBufSize) {
            handle.dicPos = 0;
        }

        source->Skip(bufferSize);
    }
    YCHECK(status == LZMA_STATUS_FINISHED_WITH_MARK);
    LzmaDec_Free(&handle, &Alloc);
}

} // namespace NYT::NCompression
