#include "random_access_gzip.h"

#include <yt/core/misc/assert.h>

namespace NYT {
namespace NLogging {

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 1)

struct TGZipFixedHeader
{
    ui8 Id[2];
    char Uninteresting0[1];
    char Flags;
    char Uninteresting1[6];
};

struct TGZipExtendedHeader
{
    TGZipFixedHeader FixedHeader;
    ui16 XLen;
    char SubfieldId[2];
    ui16 SubfieldLength;
    ui32 SmuggledBlockSize;
};

constexpr int HeaderGrowth = sizeof(TGZipExtendedHeader) - sizeof(TGZipFixedHeader);
constexpr int ExtraFlag = 1 << 2;

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

TRandomAccessGZipFile::TRandomAccessGZipFile(const TString& path, int blockSize)
    : File_(path, OpenAlways|RdWr|CloseOnExec)
    , BlockSize_(blockSize)
{
    Repair();
    Reset();
}

void TRandomAccessGZipFile::Repair()
{
    auto fileSize = File_.GetLength();
    if (fileSize == 0) {
        return;
    }

    while (OutputPosition_ != fileSize) {
        TGZipExtendedHeader header;
        if (fileSize - OutputPosition_ < sizeof(header)) {
            File_.Resize(OutputPosition_);
            return;
        }
        
        File_.Pread(&header, sizeof(header), OutputPosition_);
        // Wrong magic.
        if (header.FixedHeader.Id[0] != 0x1f || header.FixedHeader.Id[1] != 0x8b) {
            File_.Resize(OutputPosition_);
            return;
        }

        // Block is not fully flushed.
        if (OutputPosition_ + header.SmuggledBlockSize > fileSize || header.SmuggledBlockSize == 0) {
            File_.Resize(OutputPosition_);
            return;
        }

        OutputPosition_ += header.SmuggledBlockSize;
    }
}

void TRandomAccessGZipFile::Reset()
{
    Output_.Buffer().Resize(HeaderGrowth);
    Compressor_.reset(new TZLibCompress(&Output_, ZLib::GZip));
}

void TRandomAccessGZipFile::DoWrite(const void* buf, size_t len)
{
    Compressor_->Write(buf, len);
}

void TRandomAccessGZipFile::DoFlush()
{
    Compressor_->Finish();
    auto buffer = Output_.Buffer();
    
    TGZipExtendedHeader header;
    memcpy(&header.FixedHeader, buffer.Data() + HeaderGrowth, sizeof(header.FixedHeader));

    YCHECK(header.FixedHeader.Id[0] == 0x1f);
    YCHECK(header.FixedHeader.Id[1] == 0x8b);
    YCHECK((header.FixedHeader.Flags & ExtraFlag) == 0);
    header.FixedHeader.Flags |= ExtraFlag;
    header.XLen = 8;
    header.SubfieldId[0] = 'Y';
    header.SubfieldId[1] = 'T';
    header.SubfieldLength = 4;
    header.SmuggledBlockSize = buffer.Size();

    memcpy(buffer.Data(), &header, sizeof(header));

    File_.Pwrite(buffer.Data(), buffer.Size(), OutputPosition_);
    OutputPosition_ += buffer.Size();
    Reset();
}

void TRandomAccessGZipFile::DoFinish()
{
    Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLogging
} // namespace NYT
