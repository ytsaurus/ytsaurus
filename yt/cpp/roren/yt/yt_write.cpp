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
    const NYT::TSortColumns& columnsToSort)
{
    auto unsortedSchema = schema;
    for (auto& column : unsortedSchema.MutableColumns()) {
        column.ResetSortOrder();
    }

    bool uniqueKeys = schema.UniqueKeys();
    unsortedSchema.UniqueKeys(false);

    auto transform = TYtWriteTransform{path, unsortedSchema, columnsToSort, uniqueKeys};
    return transform;
}

TYtWriteTransform YtSortedWrite(
    const NYT::TRichYPath& path,
    const NYT::TTableSchema& sortedSchema)
{
    TVector<TString> columnsToSort;
    for (const auto& column : sortedSchema.Columns()) {
        if (column.SortOrder()) {
            columnsToSort.push_back(column.Name());
        }
    }

    return YtSortedWrite(path, sortedSchema, NYT::TSortColumns(columnsToSort));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
