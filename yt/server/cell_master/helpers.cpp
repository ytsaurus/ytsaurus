#include "helpers.h"

#include <yt/server/cell_master/multicell_manager.pb.h>

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
