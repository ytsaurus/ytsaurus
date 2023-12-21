#pragma once

#include <yt/cpp/roren/yt/transforms.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

TYtWriteTransform YtWrite(const NYT::TRichYPath& path, const NYT::TTableSchema& schema);

////////////////////////////////////////////////////////////////////////////////

TYtWriteTransform YtSortedWrite(
    const NYT::TRichYPath& path,
    const NYT::TTableSchema& schema,
    const NYT::TSortColumns& columnsToSort);

TYtWriteTransform YtSortedWrite(
    const NYT::TRichYPath& path,
    const NYT::TTableSchema& sortedSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
