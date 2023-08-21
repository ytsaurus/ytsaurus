#include "cell_statistics.h"

#include <yt/yt/server/master/cell_master/proto/multicell_manager.pb.h>

#include <library/cpp/yt/string/format.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

NProto::TCellStatistics& operator += (NProto::TCellStatistics& lhs, const NProto::TCellStatistics& rhs)
{
    #define XX(name) lhs.set_##name(lhs.name() + rhs.name());
    ITERATE_CELL_STATISTICS(XX)
    #undef XX
    return lhs;
}

NProto::TCellStatistics operator +  (const NProto::TCellStatistics& lhs, const NProto::TCellStatistics& rhs)
{
    NProto::TCellStatistics result;
    #define XX(name) result.set_##name(lhs.name() + rhs.name());
    ITERATE_CELL_STATISTICS(XX)
    #undef XX
    return result;
}

namespace NProto {

void FormatValue(TStringBuilderBase* builder, const NProto::TCellStatistics& statistics, TStringBuf /*format*/)
{
    builder->AppendFormat("ChunkCount: %v, LostVitalChunkCount: %v, OnlineNodeCount: %v",
        statistics.chunk_count(),
        statistics.lost_vital_chunk_count(),
        statistics.online_node_count());
}

TString ToString(const NProto::TCellStatistics& statistics)
{
    return ToStringViaBuilder(statistics);
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
