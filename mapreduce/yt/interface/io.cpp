#include "io.h"

#include <mapreduce/yt/interface/logging/log.h>

#include <util/string/cast.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TMaybe<size_t> IReaderImplBase::GetReadByteCount() const
{
    return Nothing();
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

void LogTableReaderStatistics(ui64 rowCount, TMaybe<size_t> byteCount)
{
    TString byteCountStr = (byteCount ? ::ToString(*byteCount) : "<unknown>");
    LOG_DEBUG("Table reader has read %" PRIu64 " rows, %s bytes",
        rowCount,
        ~byteCountStr);
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
