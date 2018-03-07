#include "data_statistics.h"

#include <yt/core/misc/format.h>

#include <yt/core/ytree/fluent.h>

#include <yt/ytlib/chunk_client/public.h>

namespace NYT {
namespace NChunkClient {
namespace NProto {

using namespace NYTree;
using namespace NYson;

using ::ToString;
using ::FromString;

////////////////////////////////////////////////////////////////////////////////

bool HasInvalidDataWeight(const TDataStatistics& statistics)
{
    return statistics.has_data_weight() && statistics.data_weight() == -1;
}

TDataStatistics& operator += (TDataStatistics& lhs, const TDataStatistics& rhs)
{
    lhs.set_uncompressed_data_size(lhs.uncompressed_data_size() + rhs.uncompressed_data_size());
    lhs.set_compressed_data_size(lhs.compressed_data_size() + rhs.compressed_data_size());
    lhs.set_chunk_count(lhs.chunk_count() + rhs.chunk_count());
    lhs.set_row_count(lhs.row_count() + rhs.row_count());
    lhs.set_regular_disk_space(lhs.regular_disk_space() + rhs.regular_disk_space());
    lhs.set_erasure_disk_space(lhs.erasure_disk_space() + rhs.erasure_disk_space());

    if (HasInvalidDataWeight(lhs) || HasInvalidDataWeight(rhs)) {
        lhs.set_data_weight(-1);
    } else {
        lhs.set_data_weight(lhs.data_weight() + rhs.data_weight());
    }

    return lhs;
}

TDataStatistics  operator +  (const TDataStatistics& lhs, const TDataStatistics& rhs)
{
    auto result = lhs;
    result += rhs;
    return result;
}

TDataStatistics& operator -= (TDataStatistics& lhs, const TDataStatistics& rhs)
{
    lhs.set_uncompressed_data_size(lhs.uncompressed_data_size() - rhs.uncompressed_data_size());
    lhs.set_compressed_data_size(lhs.compressed_data_size() - rhs.compressed_data_size());
    lhs.set_chunk_count(lhs.chunk_count() - rhs.chunk_count());
    lhs.set_row_count(lhs.row_count() - rhs.row_count());
    lhs.set_regular_disk_space(lhs.regular_disk_space() - rhs.regular_disk_space());

    if (HasInvalidDataWeight(lhs) || HasInvalidDataWeight(rhs)) {
        lhs.set_data_weight(-1);
    } else {
        lhs.set_data_weight(lhs.data_weight() - rhs.data_weight());
    }

    return lhs;
}

TDataStatistics  operator - (const TDataStatistics& lhs, const TDataStatistics& rhs)
{
    auto result = lhs;
    result -= rhs;
    return result;
}

bool operator == (const TDataStatistics& lhs, const TDataStatistics& rhs)
{
    return
        lhs.uncompressed_data_size() == rhs.uncompressed_data_size() &&
        lhs.compressed_data_size() == rhs.compressed_data_size() &&
        lhs.row_count() == rhs.row_count() &&
        lhs.chunk_count() == rhs.chunk_count() &&
        lhs.regular_disk_space() == rhs.regular_disk_space() &&
        lhs.erasure_disk_space() == rhs.erasure_disk_space() &&
        (HasInvalidDataWeight(lhs) || HasInvalidDataWeight(rhs) || lhs.data_weight() == rhs.data_weight());
}

bool operator != (const TDataStatistics& lhs, const TDataStatistics& rhs)
{
    return !(lhs == rhs);
}

void Serialize(const TDataStatistics& statistics, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginMap()
        .Item("chunk_count").Value(statistics.chunk_count())
        .Item("row_count").Value(statistics.row_count())
        .Item("uncompressed_data_size").Value(statistics.uncompressed_data_size())
        .Item("compressed_data_size").Value(statistics.compressed_data_size())
        .Item("data_weight").Value(statistics.data_weight())
        .Item("regular_disk_space").Value(statistics.regular_disk_space())
        .Item("erasure_disk_space").Value(statistics.erasure_disk_space())
    .EndMap();
}

void Deserialize(TDataStatistics& value, INodePtr node)
{
    auto rootMap = node->AsMap();
    value.set_chunk_count(rootMap->GetChild("chunk_count")->GetValue<i64>());
    value.set_row_count(rootMap->GetChild("row_count")->GetValue<i64>());
    value.set_uncompressed_data_size(rootMap->GetChild("uncompressed_data_size")->GetValue<i64>());
    value.set_compressed_data_size(rootMap->GetChild("compressed_data_size")->GetValue<i64>());
    value.set_data_weight(rootMap->GetChild("data_weight")->GetValue<i64>());

    value.set_regular_disk_space(rootMap->GetChild("regular_disk_space")->GetValue<i64>());
    value.set_erasure_disk_space(rootMap->GetChild("erasure_disk_space")->GetValue<i64>());
}

void SetDataStatisticsField(TDataStatistics& statistics, TStringBuf key, i64 value) {
    if (key == "chunk_count") {
        statistics.set_chunk_count(value);
    } else if (key == "row_count") {
        statistics.set_row_count(value);
    } else if (key == "uncompressed_data_size") {
        statistics.set_uncompressed_data_size(value);
    } else if (key == "compressed_data_size") {
        statistics.set_compressed_data_size(value);
    } else if (key == "data_weight") {
        statistics.set_data_weight(value);
    } else if (key == "regular_disk_space") {
        statistics.set_regular_disk_space(value);
    } else if (key == "erasure_disk_space") {
        statistics.set_erasure_disk_space(value);
    } // Else we have a strange situation on our hands but we intentionally ignore it.
}

TString ToString(const TDataStatistics& statistics)
{
    return Format(
        "{UncompressedDataSize: %v, CompressedDataSize: %v, DataWeight: %v, RowCount: %v, "
        "ChunkCount: %v, RegularDiskSpace: %v, ErasureDiskSpace: %v",
        statistics.uncompressed_data_size(),
        statistics.compressed_data_size(),
        statistics.data_weight(),
        statistics.row_count(),
        statistics.chunk_count(),
        statistics.regular_disk_space(),
        statistics.erasure_disk_space());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProto
} // namespace NChunkClient
} // namespace NYT
