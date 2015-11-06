#include "lzop.h"

#include <contrib/z-lz-lzo/minilzo.h>

#include <util/generic/buffer.h>
#include <util/system/info.h>

// See https://svn.yandex.ru/statbox/packages/yandex/statbox-binaries/include/Statbox/LZOP.h
// As the source for the inspiration.
////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {
namespace NLzop {

static unsigned const char MAGIC[9] =
{
    0x89, 0x4c, 0x5a, 0x4f,
    0x00,
    0x0d, 0x0a, 0x1a, 0x0a
};

// 32-bit version.
inline unsigned int RoundUpToPow2(unsigned int x)
{
    x -= 1;
    x |= (x >> 1);
    x |= (x >> 2);
    x |= (x >> 4);
    x |= (x >> 8);
    x |= (x >> 16);
    return x + 1;
}

inline unsigned char* get8(unsigned char* p, unsigned* v, ui32* adler32, ui32* crc32)
{
    *v = 0;
    *v |= (unsigned)(*p++);

    *adler32 = lzo_adler32(*adler32, (const unsigned char*)(p - 1), 1);
    *crc32   = lzo_crc32(*crc32, (const unsigned char*)(p - 1), 1);

    return p;
}

inline unsigned char* put8(unsigned char* p, unsigned v, ui32* adler32, ui32* crc32)
{
    *p++ = v & 0xff;

    *adler32 = lzo_adler32(*adler32, (const unsigned char*)(p - 1), 1);
    *crc32   = lzo_crc32(*crc32, (const unsigned char*)(p - 1), 1);

    return p;
}

inline unsigned char* get16(unsigned char* p, unsigned* v, ui32* adler32, ui32* crc32)
{
    *v = 0;
    *v |= (unsigned)(*p++) << 8;
    *v |= (unsigned)(*p++);

    *adler32 = lzo_adler32(*adler32, (const unsigned char*)(p - 2), 2);
    *crc32   = lzo_crc32(*crc32, (const unsigned char*)(p - 2), 2);

    return p;
}

inline unsigned char* put16(unsigned char* p, unsigned v, ui32* adler32, ui32* crc32)
{
    *p++ = (v >> 8) & 0xff;
    *p++ = (v     ) & 0xff;

    *adler32 = lzo_adler32(*adler32, (const unsigned char*)(p - 2), 2);
    *crc32   = lzo_crc32(*crc32, (const unsigned char*)(p - 2), 2);

    return p;
}

inline unsigned char* get32(unsigned char* p, unsigned* v, ui32* adler32, ui32* crc32)
{
    *v = 0;
    *v |= (unsigned)(*p++) << 24;
    *v |= (unsigned)(*p++) << 16;
    *v |= (unsigned)(*p++) << 8;
    *v |= (unsigned)(*p++);

    *adler32 = lzo_adler32(*adler32, (const unsigned char*)(p - 4), 4);
    *crc32   = lzo_crc32(*crc32, (const unsigned char*)(p - 4), 4);

    return p;
}

inline unsigned char* put32(unsigned char* p, unsigned v, ui32* adler32, ui32* crc32)
{
    *p++ = (v >> 24) & 0xff;
    *p++ = (v >> 16) & 0xff;
    *p++ = (v >> 8 ) & 0xff;
    *p++ = (v      ) & 0xff;

    *adler32 = lzo_adler32(*adler32, (const unsigned char*)(p - 4), 4);
    *crc32   = lzo_crc32(*crc32, (const unsigned char*)(p - 4), 4);

    return p;
}

enum {
    LZO_ADLER32_D     = 0x00000001L,
    LZO_ADLER32_C     = 0x00000002L,
    LZO_STDIN         = 0x00000004L,
    LZO_STDOUT        = 0x00000008L,
    LZO_NAME_DEFAULT  = 0x00000010L,
    LZO_DOSISH        = 0x00000020L,
    LZO_H_EXTRA_FIELD = 0x00000040L,
    LZO_H_GMTDIFF     = 0x00000080L,
    LZO_CRC32_D       = 0x00000100L,
    LZO_CRC32_C       = 0x00000200L,
    LZO_MULTIPART     = 0x00000400L,
    LZO_H_FILTER      = 0x00000800L,
    LZO_H_CRC32       = 0x00001000L,
    LZO_H_PATH        = 0x00002000L,
    LZO_MASK          = 0x00003FFFL
};

enum
{
    LZO_END_OF_STREAM            = 0,
    LZO_MORE_DATA                = 1,
    LZO_OK                       = 2,
    LZO_WRONG_MAGIC              = -12,
    LZO_VERSION_TOO_LOW          = -13,
    LZO_EXTRACT_VERSION_TOO_HIGH = -14,
    LZO_EXTRACT_VERSION_TOO_LOW  = -15,
    LZO_WRONG_CHECKSUM           = -16,
    LZO_WRONG_METHOD             = -18,
    LZO_COMPRESS_ERROR           = -1,
    LZO_WRONG_DST_LEN            = -2,
    LZO_DST_LEN_TOO_BIG          = -3,
    LZO_WRONG_SRC_LEN            = -4,
    LZO_INVALID_SRC_ADLER32      = -5,
    LZO_INVALID_SRC_CRC32        = -6,
    LZO_DECOMPRESS_ERROR         = -7,
    LZO_INVALID_DST_ADLER32      = -8,
    LZO_INVALID_DST_CRC32        = -9,
};

// XXX(sandello): I don't really know where this comes from.
struct THeader {
    unsigned version;
    unsigned lib_version;
    unsigned version_needed_to_extract;
    unsigned char method;
    unsigned char level;

    ui32 flags;
    ui32 filter;
    ui32 mode;
    ui32 mtime_low;
    ui32 mtime_high;

    ui32 header_checksum;

    ui32 extra_field_len;
    ui32 extra_field_checksum;

    const unsigned char* method_name;

    char name[255 + 1];
};

} // namespace NLzop
} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

class TLzopCompress::TImpl
    : public TOutputStream
{
public:
    inline TImpl(TOutputStream* slave, ui16 blockSize)
        : Slave(slave)
        , BlockSize(blockSize)
        , UncompressedBuffer(blockSize)
        , CompressedBuffer(8 + 4 * blockSize)
    {
        ::memset(&Header, 0, sizeof(Header));
    }

protected:
    virtual void DoWrite(const void* buffer, size_t length);
    virtual void DoFlush();
    virtual void DoFinish();

private:
    TOutputStream* Slave;
    ui16 BlockSize;

    NPrivate::NLzop::THeader Header;
    bool HeaderWasWritten;

    TBuffer UncompressedBuffer;
    TBuffer CompressedBuffer;

    void EnsureCompressedSpace(size_t demand);
    void EnsureUncompressedSpace(size_t demand);

    void ProduceHeader();
    void ProduceData();
    void ProduceTrailer();
};

void TLzopCompress::TImpl::DoWrite(const void* buffer, size_t length)
{
    const char* data = (const char*)buffer;
    while (length > 0) {
        size_t bytesToFit = Min(UncompressedBuffer.Capacity(), length);
        size_t bytesToWrite = Min(UncompressedBuffer.Avail(), length);
        if (bytesToWrite > 0) {
            UncompressedBuffer.Append(data, bytesToWrite);
            data   += bytesToWrite;
            length -= bytesToWrite;
        } else {
            EnsureUncompressedSpace(bytesToFit);
        }
    }
}

void TLzopCompress::TImpl::DoFlush()
{
    EnsureUncompressedSpace(UncompressedBuffer.Capacity());
    EnsureCompressedSpace(CompressedBuffer.Capacity());
}

void TLzopCompress::TImpl::DoFinish()
{
    EnsureUncompressedSpace(UncompressedBuffer.Capacity());
    ProduceTrailer();
    Flush();
}

void TLzopCompress::TImpl::EnsureCompressedSpace(size_t demand)
{
    YASSERT(demand <= CompressedBuffer.Capacity());
    if (CompressedBuffer.Avail() < demand) {
        Slave->Write(CompressedBuffer.Data(), CompressedBuffer.Size());
        CompressedBuffer.Clear();
    }
    YASSERT(demand <= CompressedBuffer.Avail());
}

void TLzopCompress::TImpl::EnsureUncompressedSpace(size_t demand)
{
    YASSERT(demand <= UncompressedBuffer.Capacity());
    if (UncompressedBuffer.Avail() < demand) {
        ProduceData();
    }
    YASSERT(demand <= UncompressedBuffer.Avail());
}

void TLzopCompress::TImpl::ProduceHeader()
{
    using namespace NPrivate::NLzop;

    ui32 adler32 = 1;
    ui32 crc32   = 0;

    unsigned char* p;
    unsigned char* pb;

    EnsureCompressedSpace(sizeof(MAGIC) + sizeof(Header));
    pb = p = (unsigned char*)CompressedBuffer.Pos();

    // Magic.
    ::memcpy(p, MAGIC, sizeof(MAGIC)); p+= sizeof(MAGIC);

    // .version
    p = put16(p, 0x1030U, &adler32, &crc32);
    // .lib_version
    p = put16(p, lzo_version() & 0xFFFFU, &adler32, &crc32);
    // .version_needed_to_extract
    p = put16(p, 0x0900, &adler32, &crc32);
    // .method
    // XXX(sandello): Method deviates from Statbox' implementation.
    // In compatibility we trust.
    p = put8(p, 1, &adler32, &crc32); // 1 = LZO1X_1, 2 = LZO1X_1_15
    // .level
    p = put8(p, 3, &adler32, &crc32);
    // .flags
    p = put32(p, 0, &adler32, &crc32);
    // .mode
    p = put32(p, 0644, &adler32, &crc32);
    // .mtime_low
    p = put32(p, 0, &adler32, &crc32);
    // .mtime_high
    p = put32(p, 0, &adler32, &crc32);
    // .name
    p = put8(p, 0, &adler32, &crc32);
    // .header_checksum
    p = put32(p, adler32, &adler32, &crc32);

    CompressedBuffer.Proceed(CompressedBuffer.Size() + (p - pb));
}

void TLzopCompress::TImpl::ProduceTrailer()
{
    using namespace NPrivate::NLzop;

    ui32 adler32 = 1;
    ui32 crc32   = 0;

    unsigned char* p;
    unsigned char* pb;

    EnsureCompressedSpace(4);
    pb = p = (unsigned char*)CompressedBuffer.Pos();

    p = put32(p, 0, &adler32, &crc32);

    CompressedBuffer.Proceed(CompressedBuffer.Size() + (p - pb));
}

void TLzopCompress::TImpl::ProduceData()
{
    using namespace NPrivate::NLzop;

    ui32 src_len = (ui32)UncompressedBuffer.Size();
    ui32 dst_len;

    unsigned char *p;
    unsigned char *pb;

    lzo_uint result;

    // See include/lzo/lzo1x.h from lzo-2.06.
    // const size_t LZO1X_1_MEM_COMPRESS = (lzo_uint32)(16384L * lzo_sizeof_dict_t);
    unsigned char scratch[LZO1X_1_MEM_COMPRESS];

    if (!HeaderWasWritten) {
        ProduceHeader();
        HeaderWasWritten = true;
    }

    EnsureCompressedSpace(8 + 4 * src_len);
    pb = p = (unsigned char*)CompressedBuffer.Pos();

    p  = put32(p, src_len, 0, 0);
    p += 4;

    // XXX(sandello): Used compression method deviates from Statbox's implementation.
    // Here we use |lzo1x_1_compress| (implemented in minilzo) whilst Statbox
    // uses |lzo1x_1_15_compress|.
    if (lzo1x_1_compress(
        (unsigned char*)UncompressedBuffer.Data(),
        UncompressedBuffer.Size(),
        p,
        &result,
        scratch) != LZO_E_OK)
    {
        ythrow yexception() << "LZOP Error: " << (int)LZO_COMPRESS_ERROR;
    }

    dst_len = result;

    if (dst_len < src_len) {
        put32(pb + 4, dst_len, 0, 0);
        /**/
        result = dst_len;
    } else {
        put32(pb + 4, src_len, 0, 0);
        ::memcpy(p, UncompressedBuffer.Data(), UncompressedBuffer.Size());
        result = src_len;
    }

    result += 4 + 4; // src_len + dst_len + (adler32|crc32, disabled)

    UncompressedBuffer.Clear();
    CompressedBuffer.Proceed(CompressedBuffer.Size() + result);
}

TLzopCompress::TLzopCompress(TOutputStream* slave, ui16 maxBlockSize)
    : Impl_(new TImpl(slave, maxBlockSize))
{ }

TLzopCompress::~TLzopCompress() throw()
{
    try {
        Finish();
    } catch (...) {
    }
}

void TLzopCompress::DoWrite(const void* buffer, size_t length)
{
    if (!Impl_) {
        ythrow yexception() << "Stream is dead";
    }
    Impl_->Write((const char*)buffer, length);
}

void TLzopCompress::DoFlush() {
    if (!Impl_) {
        ythrow yexception() << "Stream is dead";
    }
    Impl_->Flush();
}

void TLzopCompress::DoFinish() {
    THolder<TImpl> impl(Impl_.Release());
    if (!!impl) {
        impl->Finish();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TLzopDecompress::TImpl
    : public TInputStream
{
public:
    inline TImpl(TInputStream* slave, ui32 initialBufferSize)
        : Slave(slave)
        , Exhausted(false)
        , Hint(0)
        , InputData(NPrivate::NLzop::RoundUpToPow2(initialBufferSize))
        , OutputData(NPrivate::NLzop::RoundUpToPow2(initialBufferSize))
        , InputOffset(0)
        , OutputOffset(0)
    {
        ::memset(&Header, 0, sizeof(Header));
    }

protected:
    virtual size_t DoRead(void* buffer, size_t length);

private:
    TInputStream* Slave;
    bool Exhausted;
    unsigned int Hint;

    NPrivate::NLzop::THeader Header;

    TBuffer InputData;
    TBuffer OutputData;

    size_t InputOffset;
    size_t OutputOffset;

    void Trim(TBuffer& buffer, size_t& length);

    int ConsumeHeader();
    int ConsumeData();
};

size_t TLzopDecompress::TImpl::DoRead(void* buffer, size_t length)
{
    size_t bytesRead = 0;
    size_t bytesAvailable = 0;

    do {
        bytesAvailable = Min(OutputData.Size() - OutputOffset, length);
        if (!bytesAvailable && !Exhausted) {
            int rv;
            while ((rv = ConsumeData()) == NPrivate::NLzop::LZO_MORE_DATA) {
                if (Hint) {
                    InputData.Reserve(NPrivate::NLzop::RoundUpToPow2(Hint));
                    Hint = 0;
                } else if (InputData.Avail() == 0) {
                    InputData.Reserve(2 * InputData.Capacity());
                }

                size_t tmp = Slave->Load(InputData.Pos(), InputData.Avail());
                if (tmp) {
                    InputData.Advance(tmp);
                } else {
                    Exhausted = true;
                    break;
                }
            }

            Trim(InputData, InputOffset);

            switch (rv) {
                case NPrivate::NLzop::LZO_OK:
                case NPrivate::NLzop::LZO_MORE_DATA:
                    break;
                case NPrivate::NLzop::LZO_END_OF_STREAM:
                    Exhausted = true;
                    break;
                default:
                    ythrow yexception() << "LZOP Error: " << rv;
                    break;
            }
        } else if (bytesAvailable) {
            ::memcpy(
                (char*)buffer + bytesRead,
                OutputData.Data() + OutputOffset,
                bytesAvailable);
            bytesRead += bytesAvailable;
            OutputOffset += bytesAvailable;

            Trim(OutputData, OutputOffset);
        } else {
            break;
        }
    } while(!bytesRead);

    return bytesRead;
}

void TLzopDecompress::TImpl::Trim(TBuffer& buffer, size_t& length)
{
    size_t remaining = buffer.Size() - length;
    ::memmove(
        buffer.Data(),
        buffer.Data() + length,
        remaining);
    buffer.Resize(remaining);
    length = 0;
}

int TLzopDecompress::TImpl::ConsumeHeader()
{
    using namespace NPrivate::NLzop;

    THeader* h = &Header;

    ui32 adler32 = 1;
    ui32 crc32   = 0;
    ui32 checksum;

    unsigned tmp;

    unsigned char* p;
    unsigned char* pb;
    unsigned char* pe;

    pb = p = (unsigned char*)InputData.Data() + InputOffset;
    pe = (unsigned char*)InputData.Pos();

    // Magic.
    if (pe < p + sizeof(MAGIC)) return LZO_MORE_DATA;
    if (memcmp(MAGIC, p, sizeof(MAGIC)) != 0) {
        return LZO_WRONG_MAGIC;
    }
    p += sizeof(MAGIC);

    // .version
    if (pe - p < 2) return LZO_MORE_DATA;
    p = get16(p, &h->version, &adler32, &crc32);
    if (h->version < 0x0900) {
        return LZO_VERSION_TOO_LOW;
    }

    // .lib_version, .version_needed_to_extract
    if (pe - p < 2) return LZO_MORE_DATA;
    p = get16(p, &h->lib_version, &adler32, &crc32);
    if (h->version >= 0x0940)
    {
        if (pe - p < 2) return LZO_MORE_DATA;
        p = get16(p, &h->version_needed_to_extract, &adler32, &crc32);
        if (h->version_needed_to_extract > 0x1030) {
            return LZO_EXTRACT_VERSION_TOO_HIGH;
        }
        if (h->version_needed_to_extract < 0x0900) {
            return LZO_EXTRACT_VERSION_TOO_LOW;
        }
    }

    // .method, .level
    if (pe - p < 1) return LZO_MORE_DATA;
    p = get8(p, &tmp, &adler32, &crc32);
    h->method = tmp;
    if (h->version >= 0x0940) {
        if (pe - p < 1) return LZO_MORE_DATA;
        p = get8(p, &tmp, &adler32, &crc32);
        h->level = tmp;
    }

    // .flags
    if (pe - p < 4) return LZO_MORE_DATA;
    p = get32(p, &h->flags, &adler32, &crc32);

    // .filter
    if (h->flags & LZO_H_FILTER) {
        if (pe - p < 4) return LZO_MORE_DATA;
        p = get32(p, &h->filter, &adler32, &crc32);
    }

    // .mode
    if (pe - p < 4) return LZO_MORE_DATA;
    p = get32(p, &h->mode, &adler32, &crc32);

    // .mtime_low
    if (pe - p < 4) return LZO_MORE_DATA;
    p = get32(p, &h->mtime_low, &adler32, &crc32);

    // .mtime_high
    if (h->version >= 0x0940) {
        if (pe - p < 4) return LZO_MORE_DATA;
        p = get32(p, &h->mtime_high, &adler32, &crc32);
    }
    if (h->version < 0x0120) {
        if (h->mtime_low == 0xffffffffUL) {
            h->mtime_low = 0;
        }
        h->mtime_high = 0;
    }

    // .name
    if (pe - p < 1) return LZO_MORE_DATA;
    p = get8(p, &tmp, &adler32, &crc32);
    if (tmp > 0) {
        if (pe - p < tmp) return LZO_MORE_DATA;
        adler32 = lzo_adler32(adler32, p, tmp);
        crc32   = lzo_crc32(crc32, p, tmp);

        ::memcpy(h->name, p, tmp); p += tmp;
    }

    if (h->flags & LZO_H_CRC32) {
        checksum = crc32;
    } else {
        checksum = adler32;
    }

    // .header_checksum
    if (pe - p < 4) return LZO_MORE_DATA;
    p = get32(p, &h->header_checksum, &adler32, &crc32);
    if (h->header_checksum != checksum) {
        return LZO_WRONG_CHECKSUM;
    }

    // XXX(sandello): This is internal Statbox constraint.
    if (h->method != 2) {
        return LZO_WRONG_METHOD;
    }

    if (h->flags & LZO_H_EXTRA_FIELD) {
        if (pe - p < 4) return LZO_MORE_DATA;
        p = get32(p, &h->extra_field_len, &adler32, &crc32);
        if (pe - p < h->extra_field_len) return LZO_MORE_DATA;
        p += h->extra_field_len;
    }

    // OK
    InputOffset += p - pb;
    return LZO_OK;
}

int TLzopDecompress::TImpl::ConsumeData()
{
    using namespace NPrivate::NLzop;

    THeader* h = &Header;

    ui32 adler32   = 1;
    ui32 crc32     = 0;

    ui32 d_adler32 = 1;
    ui32 d_crc32   = 0;
    ui32 c_adler32 = 1;
    ui32 c_crc32   = 0;

    ui32 dst_len;
    ui32 src_len;

    unsigned char* p;
    unsigned char* pb;
    unsigned char* pe;

    if (h->version == 0) {
        return ConsumeHeader();
    }

    pb = p = (unsigned char*)InputData.Data() + InputOffset;
    pe = (unsigned char*)InputData.Pos();

    // dst_len
    if (pe - p < 4) return LZO_MORE_DATA;
    p = get32(p, &dst_len, &adler32, &crc32);

    if (dst_len == 0) {
        InputOffset += p - pb;
        return LZO_END_OF_STREAM;
    }
    if (dst_len == 0xffffffffUL) {
        return LZO_WRONG_DST_LEN;
    }
    if (dst_len > 64 * 1024 * 1024) {
        return LZO_DST_LEN_TOO_BIG;
    }

    // src_len
    if (pe - p < 4) return LZO_MORE_DATA;
    p = get32(p, &src_len, &adler32, &crc32);

    if (src_len <= 0 || src_len > dst_len) {
        return LZO_WRONG_SRC_LEN;
    }

    if (h->flags & LZO_ADLER32_D) {
        if (pe - p < 4) return LZO_MORE_DATA;
        p = get32(p, &d_adler32, &adler32, &crc32);
    }
    if (h->flags & LZO_CRC32_D) {
        if (pe - p < 4) return LZO_MORE_DATA;
        p = get32(p, &d_crc32, &adler32, &crc32);
    }

    if (h->flags & LZO_ADLER32_C) {
        if (src_len < dst_len) {
            if (pe - p < 4) return LZO_MORE_DATA;
            p = get32(p, &c_adler32, &adler32, &crc32);
        } else {
            if (!(h->flags & LZO_ADLER32_D))
                ythrow yexception() << "h->flags & LZO_ADLER32_C & ~LZO_ADLER32_D";
            c_adler32 = d_adler32;
        }
    }
    if (h->flags & LZO_CRC32_C) {
        if (src_len < dst_len) {
            if (pe - p < 4) return LZO_MORE_DATA;
            p = get32(p, &c_crc32, &adler32, &crc32);
        } else {
            if (!(h->flags & LZO_CRC32_D))
                ythrow yexception() << "h->flags & LZO_CRC32_C & ~LZO_CRC32_D";
            c_crc32 = d_crc32;
        }
    }

    // Rock'n'roll! Check'n'consume!
    if (pe - p < src_len) {
        Hint = (p - pb) + src_len;
        return LZO_MORE_DATA;
    }

    if (h->flags & LZO_ADLER32_C) {
        ui32 checksum;
        checksum = lzo_adler32(1, p, src_len);
        if (checksum != c_adler32) {
            return LZO_INVALID_SRC_ADLER32;
        }
    }
    if (h->flags & LZO_CRC32_C) {
        ui32 checksum;
        checksum = lzo_crc32(1, p, src_len);
        if (checksum != c_crc32) {
            return LZO_INVALID_SRC_CRC32;
        }
    }

    if (OutputData.Avail() < dst_len) {
        OutputData.Reserve(RoundUpToPow2(2 * (OutputData.Size() + dst_len)));
    }

    unsigned char* output = (unsigned char*)OutputData.Pos();
    OutputData.Advance(dst_len);

    if (src_len < dst_len) {
        lzo_uint tmp;
        int rv;

        tmp = dst_len;
        rv = lzo1x_decompress_safe(
            p,
            src_len,
            output,
            &tmp,
            0);

        if (rv != LZO_E_OK || tmp != dst_len) {
            return LZO_DECOMPRESS_ERROR;
        }
    } else {
        if (!(dst_len == src_len)) {
            ythrow yexception() << "dst_len == src_len";
        }
        ::memcpy(output, p, src_len);
    }

    p += src_len;

    // Check again.
    if (h->flags & LZO_ADLER32_D) {
        ui32 checksum;
        checksum = lzo_adler32(1, output, dst_len);
        if (checksum != d_adler32) {
            return LZO_INVALID_DST_ADLER32;
        }
    }
    if (h->flags & LZO_CRC32_D) {
        ui32 checksum;
        checksum = lzo_crc32(1, output, dst_len);
        if (checksum != d_crc32) {
            return LZO_INVALID_DST_CRC32;
        }
    }

    // OK
    InputOffset += p - pb;
    return LZO_OK;
}

TLzopDecompress::TLzopDecompress(TInputStream* slave, ui32 initialBufferSize)
    : Impl_(new TImpl(slave, initialBufferSize))
{ }

TLzopDecompress::~TLzopDecompress() throw()
{ }

size_t TLzopDecompress::DoRead(void* buffer, size_t length)
{
    return Impl_->Read(buffer, length);
}

