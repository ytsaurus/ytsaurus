#pragma once

#include "public.h"
#include "ref.h"

#include <util/stream/input.h>
#include <util/stream/output.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TChecksum GetChecksum(const TRef& data);

TChecksum CombineChecksums(const std::vector<TChecksum>& blockChecksums);

namespace NDetail {
namespace NCrcSSE0xE543279765927881 {

ui64 Crc(const void* buf, size_t buflen, ui64 seed);

} // namespace NCrcSSE0xE543279765927881

namespace NCrcTable0xE543279765927881 {

ui64 Crc(const void* buf, size_t buflen, ui64 crcinit);

} // namespace NCrcTable0xE543279765927881
} // namespace NDetail

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
    TInputStream* const Input_;
    TChecksum Checksum_ = 0;
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
    TOutputStream* const Output_;
    TChecksum Checksum_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
