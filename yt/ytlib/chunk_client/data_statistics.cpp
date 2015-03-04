#include "stdafx.h"
#include "data_statistics.h"

#include <core/misc/format.h>
        
#include <core/ytree/fluent.h>

namespace NYT {
namespace NChunkClient {
namespace NProto {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TDataStatistics& operator += (TDataStatistics& lhs, const TDataStatistics& rhs)
{
    lhs.set_uncompressed_data_size(lhs.uncompressed_data_size() + rhs.uncompressed_data_size());
    lhs.set_compressed_data_size(lhs.compressed_data_size() + rhs.compressed_data_size());
    lhs.set_chunk_count(lhs.chunk_count() + rhs.chunk_count());
    lhs.set_row_count(lhs.row_count() + rhs.row_count());
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
    return lhs;
}

TDataStatistics  operator -  (const TDataStatistics& lhs, const TDataStatistics& rhs)
{
    auto result = lhs;
    result -= rhs;
    return result;
}

bool operator==  (const TDataStatistics& lhs, const TDataStatistics& rhs)
{
    return lhs.uncompressed_data_size() == rhs.compressed_data_size() &&
        lhs.compressed_data_size() == rhs.compressed_data_size() &&
        lhs.row_count() == rhs.row_count() &&
        lhs.chunk_count() == rhs.chunk_count();
}

TDataStatistics GetZeroDataStatistics()
{
    TDataStatistics dataStatistics;
    dataStatistics.set_chunk_count(0);
    dataStatistics.set_row_count(0);
    dataStatistics.set_compressed_data_size(0);
    dataStatistics.set_uncompressed_data_size(0);
    return dataStatistics;
}

const TDataStatistics& ZeroDataStatistics()
{
    static const TDataStatistics dataStatistics = GetZeroDataStatistics();
    return dataStatistics;
}

void Serialize(const TDataStatistics& statistics, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("chunk_count").Value(statistics.chunk_count())
            .Item("row_count").Value(statistics.row_count())
            .Item("uncompressed_data_size").Value(statistics.uncompressed_data_size())
            .Item("compressed_data_size").Value(statistics.compressed_data_size())
        .EndMap();
}

Stroka ToString(const TDataStatistics& statistics)
{
    return Format(
        "UncompressedDataSize: %v, CompressedDataSize: %v, RowCount: %v, ChunkCount: %v",
        statistics.uncompressed_data_size(),
        statistics.compressed_data_size(),
        statistics.row_count(),
        statistics.chunk_count());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProto
} // namespace NChunkClient
} // namespace NYT

