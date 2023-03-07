#pragma once

#include "public.h"
#include "ref.h"

#include <util/stream/input.h>
#include <util/stream/output.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TChecksum GetChecksum(TRef data);
TChecksum CombineChecksums(const std::vector<TChecksum>& blockChecksums);

////////////////////////////////////////////////////////////////////////////////

class TChecksumInput
    : public IInputStream
{
public:
    explicit TChecksumInput(IInputStream* input);
    TChecksum GetChecksum() const;

protected:
    virtual size_t DoRead(void* buf, size_t len);

private:
    IInputStream* const Input_;
    TChecksum Checksum_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TChecksumOutput
    : public IOutputStream
{
public:
    explicit TChecksumOutput(IOutputStream* output);
    TChecksum GetChecksum() const;

protected:
    virtual void DoWrite(const void* buf, size_t len);
    virtual void DoFlush();
    virtual void DoFinish();

private:
    IOutputStream* const Output_;
    TChecksum Checksum_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
