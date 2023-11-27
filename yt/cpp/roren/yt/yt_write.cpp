#include "yt_write.h"

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

TYtWriteTransform YtWrite(const NYT::TRichYPath& path, const NYT::TTableSchema& schema)
{
    return TYtWriteTransform{path, schema};
}

////////////////////////////////////////////////////////////////////////////////

TYtWriteTransform YtSortedWrite(
    const NYT::TRichYPath& path,
    const NYT::TTableSchema& schema,
    const std::vector<std::string>& columnsToSort
)
{
    return TYtWriteTransform{path, schema, columnsToSort};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
