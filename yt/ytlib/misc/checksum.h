#pragma once

#include "ref.h"

#include <util/stream/input.h>
#include <util/stream/output.h>

#define _YT_USE_CRC_8TABLE
//#define _YT_USE_CRC_PCLMUL

#ifdef _YT_USE_CRC_PCLMUL
#include <tmmintrin.h>
#include <nmmintrin.h>
#include <wmmintrin.h>

#ifdef _MSC_VER
#define DECL_PREFIX __declspec(align(16))
#define DECL_SUFFIX
#else
#define DECL_PREFIX
#define DECL_SUFFIX __attribute__((aligned(16)))
#endif
#endif


namespace NYT {


#ifdef _YT_USE_CRC_PCLMUL
namespace NCrcSSE0xE543279765927881
{
    ui64 Crc(const void* buf, size_t buflen, ui64 seed);
}
#endif


#ifdef _YT_USE_CRC_8TABLE
namespace NCrcTable0xE543279765927881
{
    ui64 Crc(const void* buf, size_t buflen, ui64 crcinit);
}
#endif

////////////////////////////////////////////////////////////////////////////////

typedef ui64 TChecksum;

TChecksum GetChecksum(TRef data);

////////////////////////////////////////////////////////////////////////////////

class TChecksumInput
    : public TInputStream
{
public:
    explicit TChecksumInput(TInputStream* input);
    TChecksum GetChecksum() const;

protected:
    virtual size_t DoRead(void* buf, size_t len);

private:
    TInputStream* Input;
    TChecksum Checksum;
};

////////////////////////////////////////////////////////////////////////////////

class TChecksumOutput
    : public TOutputStream
{
public:
    explicit TChecksumOutput(TOutputStream* output);
    TChecksum GetChecksum() const;

protected:
    virtual void DoWrite(const void* buf, size_t len);
    virtual void DoFlush();
    virtual void DoFinish();

private:
    TOutputStream* Output;
    TChecksum Checksum;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
