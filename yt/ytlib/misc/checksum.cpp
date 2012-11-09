#include "stdafx.h"
#include "checksum.h"

#include <util/digest/crc.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {

TChecksum GetChecksumImpl(const void* data, size_t length, TChecksum seed)
{
    return crc64(data, length, seed);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TChecksum GetChecksum(TRef data)
{
    return GetChecksumImpl(data.Begin(), data.Size(), 0);
}

////////////////////////////////////////////////////////////////////////////////

TChecksumInput::TChecksumInput(TInputStream* input)
    : Input(input)
    , Checksum(0)
{ }

TChecksum TChecksumInput::GetChecksum() const
{
    return Checksum;
}

size_t TChecksumInput::DoRead(void* buf, size_t len)
{
    size_t res = Input->Read(buf, len);
    Checksum = GetChecksumImpl(buf, res, Checksum);
    return res;
}

////////////////////////////////////////////////////////////////////////////////

TChecksumOutput::TChecksumOutput(TOutputStream* output)
    : Output(output)
    , Checksum(0)
{ }

TChecksum TChecksumOutput::GetChecksum() const
{
    return Checksum;
}

void TChecksumOutput::DoWrite(const void* buf, size_t len)
{
    Output->Write(buf, len);
    Checksum = GetChecksumImpl(buf, len, Checksum);
}

void TChecksumOutput::DoFlush()
{
    Output->Flush();
}

void TChecksumOutput::DoFinish()
{
    Output->Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
