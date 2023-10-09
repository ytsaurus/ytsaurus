#include "file_helpers.h"
#include "private.h"

#include <yt/yt/core/misc/fs.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

TLengthMeasuringOutputStream::TLengthMeasuringOutputStream(IOutputStream* output)
    : Output_(output)
{ }

i64 TLengthMeasuringOutputStream::GetLength() const
{
    return Length_;
}

void TLengthMeasuringOutputStream::DoWrite(const void* buf, size_t len)
{
    Output_->Write(buf, len);
    Length_ += len;
}

void TLengthMeasuringOutputStream::DoFlush()
{
    Output_->Flush();
}

void TLengthMeasuringOutputStream::DoFinish()
{
    Output_->Finish();
}

////////////////////////////////////////////////////////////////////////////////

void RemoveChangelogFiles(const TString& path)
{
    auto dataFileName = path;
    NFS::Remove(dataFileName);

    auto indexFileName = path + "." + ChangelogIndexExtension;
    if (NFS::Exists(indexFileName)) {
        NFS::Remove(indexFileName);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
