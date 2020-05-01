#include "column_format_helpers.h"

namespace NYT::NTableChunkFormat {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TUnversionedValue DoMakeUnversionedValue(i64 value, int columnId)
{
    return MakeUnversionedInt64Value(value, columnId);
}

TUnversionedValue DoMakeUnversionedValue(ui64 value, int columnId)
{
    return MakeUnversionedUint64Value(value, columnId);
}

TUnversionedValue DoMakeUnversionedValue(double value, int columnId)
{
    return MakeUnversionedDoubleValue(value, columnId);
}

TUnversionedValue DoMakeUnversionedValue(TString value, int columnId)
{
    return MakeUnversionedStringValue(value, columnId);
}

TUnversionedValue DoMakeUnversionedValue(bool value, int columnId)
{
    return MakeUnversionedBooleanValue(value, columnId);
}

TVersionedValue DoMakeVersionedValue(
    ui64 value,
    TTimestamp timestamp,
    int columnnId,
    bool aggregate)
{
    return MakeVersionedUint64Value(value, timestamp, columnnId, aggregate);
}

TVersionedValue DoMakeVersionedValue(
    i64 value,
    TTimestamp timestamp,
    int columnnId,
    bool aggregate)
{
    return MakeVersionedInt64Value(value, timestamp, columnnId, aggregate);
}

std::vector<std::pair<ui32, ui32>> GetTimestampIndexRanges(
    TRange<TVersionedRow> rows,
    TTimestamp timestamp)
{
    std::vector<std::pair<ui32, ui32>> indexRanges;
    for (auto row : rows) {
        // Find delete timestamp.
        NTableClient::TTimestamp deleteTimestamp = NTableClient::NullTimestamp;
        for (auto deleteIt = row.BeginDeleteTimestamps(); deleteIt != row.EndDeleteTimestamps(); ++deleteIt) {
            if (*deleteIt <= timestamp) {
                deleteTimestamp = std::max(*deleteIt, deleteTimestamp);
            }
        }

        ui32 lowerTimestampIndex = 0;
        while (lowerTimestampIndex < row.GetWriteTimestampCount() &&
               row.BeginWriteTimestamps()[lowerTimestampIndex] > timestamp)
        {
            ++lowerTimestampIndex;
        }

        ui32 upperTimestampIndex = lowerTimestampIndex;
        while (upperTimestampIndex < row.GetWriteTimestampCount() &&
               row.BeginWriteTimestamps()[upperTimestampIndex] > deleteTimestamp)
        {
            ++upperTimestampIndex;
        }

        indexRanges.push_back(std::make_pair(lowerTimestampIndex, upperTimestampIndex));
    }
    return indexRanges;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
