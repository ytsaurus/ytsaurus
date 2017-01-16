#pragma once

#include "public.h"

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

#define ITERATE_CELL_STATISTICS(XX) \
    XX(chunk_count) \
    XX(lost_vital_chunk_count)

NProto::TCellStatistics& operator += (NProto::TCellStatistics& lhs, const NProto::TCellStatistics& rhs);
NProto::TCellStatistics  operator +  (const NProto::TCellStatistics& lhs, const NProto::TCellStatistics& rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
