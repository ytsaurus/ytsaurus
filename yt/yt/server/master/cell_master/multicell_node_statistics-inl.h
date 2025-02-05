#ifndef MULTICELL_NODE_STATISTICS_INL_H_
#error "Direct inclusion of this file is not allowed, include multicell_node_statistics.h"
// For the sake of sane code completion.
#include "multicell_node_statistics.h"
#endif

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

// COMPAT(koloshmet)
template <std::ranges::input_range TCellStatisticPairs>
    requires std::same_as<
        std::ranges::range_value_t<TCellStatisticPairs>,
        std::pair<NObjectClient::TCellTag, NProto::TCellStatistics>>
void TMulticellNodeStatistics::PopulateMasterCellStatisticsAfterSnapshotLoaded(TCellStatisticPairs&& statisticsPairs)
{
    for (const auto& statisticsPair : statisticsPairs) {
        MasterCellStatistics_.insert(statisticsPair);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
