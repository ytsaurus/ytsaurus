#include "job_reader.h"
#include <mapreduce/yt/common/log.h>
#include <util/generic/yexception.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TJobReader::TJobReader(int fd)
    : Fd_(fd)
    , FdFile_(Duplicate(Fd_))
    , FdInput_(FdFile_)
    , BufferedInput_(&FdInput_, BUFFER_SIZE)
{ }

size_t TJobReader::DoRead(void* buf, size_t len)
{
    return BufferedInput_.Read(buf, len);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
