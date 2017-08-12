#include "job_writer.h"

#include <mapreduce/yt/interface/io.h>

#include <util/system/file.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TJobWriter::TStream::TStream(int fd)
    : FdFile(Duplicate(fd))
    , FdOutput(FdFile)
    , BufferedOutput(&FdOutput, BUFFER_SIZE)
{ }

TJobWriter::TStream::~TStream()
{
}

TJobWriter::TJobWriter(size_t outputTableCount)
{
    for (size_t i = 0; i < outputTableCount; ++i) {
        Streams_.emplace_back(MakeHolder<TStream>(int(i * 3 + 1)));
    }
}

size_t TJobWriter::GetStreamCount() const
{
    return Streams_.size();
}

IOutputStream* TJobWriter::GetStream(size_t tableIndex) const
{
    if (tableIndex >= Streams_.size()) {
        ythrow TIOException() <<
            "Table index " << tableIndex <<
            " is out of range [0, " << Streams_.size() << ")";
    }
    return &Streams_[tableIndex]->BufferedOutput;
}

void TJobWriter::OnRowFinished(size_t)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
