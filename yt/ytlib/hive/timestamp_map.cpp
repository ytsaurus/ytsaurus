#include "timestamp_map.h"

#include <yt/core/misc/serialize.h>

#include <yt/ytlib/hive/transaction_supervisor_service.pb.h>

namespace NYT {
namespace NHiveClient {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TTimestamp TTimestampMap::GetTimestamp(TCellTag cellTag) const
{
    for (const auto& pair : Timestamps) {
        if (pair.first == cellTag) {
            return pair.second;
        }
    }
    Y_UNREACHABLE();
}

void TTimestampMap::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;
    Persist<TVectorSerializer<TTupleSerializer<std::pair<TCellTag, TTimestamp>, 2>>>(
        context,
        Timestamps);
}

void ToProto(NProto::TTimestampMap* protoMap, const TTimestampMap& map)
{
    protoMap->clear_cell_tags();
    protoMap->clear_timestamps();
    for (const auto& pair : map.Timestamps) {
        protoMap->add_cell_tags(pair.first);
        protoMap->add_timestamps(pair.second);
    }
}

void FromProto(TTimestampMap* map, const NProto::TTimestampMap& protoMap)
{
    map->Timestamps.clear();
    YCHECK(protoMap.cell_tags_size() == protoMap.timestamps_size());
    for (int index = 0; index < protoMap.cell_tags_size(); ++index) {
        map->Timestamps.emplace_back(
            protoMap.cell_tags(index),
            protoMap.timestamps(index));
    }
}

void FormatValue(TStringBuilder* builder, const TTimestampMap& map, const TStringBuf& /*spec*/)
{
    builder->AppendChar('{');
    bool first = true;
    for (const auto& pair : map.Timestamps) {
        if (!first) {
            builder->AppendString(AsStringBuf(", "));
        }
        builder->AppendFormat("%v => %llx", pair.first, pair.second);
        first = false;
    }
    builder->AppendChar('}');
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveClient
} // namespace NYT
