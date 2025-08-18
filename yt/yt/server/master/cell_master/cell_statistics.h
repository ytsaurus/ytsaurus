#pragma once

#include "public.h"

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

// NB: online_node_count is intentionally omitted.
#define ITERATE_CELL_STATISTICS(XX) \
    XX(chunk_count) \
    XX(lost_vital_chunk_count) \
    XX(data_missing_chunk_count) \
    XX(parity_missing_chunk_count)

NProto::TCellStatistics& operator += (NProto::TCellStatistics& lhs, const NProto::TCellStatistics& rhs);
NProto::TCellStatistics  operator +  (const NProto::TCellStatistics& lhs, const NProto::TCellStatistics& rhs);

namespace NProto {

void FormatValue(TStringBuilderBase* builder, const TCellStatistics& statistics, TStringBuf /*spec*/);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
