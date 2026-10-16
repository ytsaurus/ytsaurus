#ifndef GOSSIP_VALUE_INL_H_
#error "Direct inclusion of this file is not allowed, include gossip_value.h"
// For the sake of sane code completion.
#include "gossip_value.h"
#endif

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
TGossipValue<TValue>::TGossipValue() = default;

template <class TValue>
TValue& TGossipValue<TValue>::Local() noexcept
{
    return *LocalPtr_;
}

template <class TValue>
const TValue& TGossipValue<TValue>::Local() const noexcept
{
    return *LocalPtr_;
}

template <class TValue>
TValue* TGossipValue<TValue>::Remote(NObjectClient::TCellTag cellTag)
{
    return &GetOrCrash(Multicell(), cellTag);
}

template <class TValue>
void TGossipValue<TValue>::Initialize(
    NObjectClient::TCellTag cellTag,
    NObjectClient::TCellTag primaryCellTag,
    const NObjectClient::TCellTagSet& secondaryCellTags,
    bool allowMasterCellRemoval)
{
    if (secondaryCellTags.empty()) {
        SetLocalPtr(&Cluster());
    } else {
        auto& multicellStatistics = Multicell();
        if (multicellStatistics.find(cellTag) == multicellStatistics.end()) {
            multicellStatistics[cellTag] = Cluster();
        }

        for (auto secondaryCellTag : secondaryCellTags) {
            multicellStatistics[secondaryCellTag];
        }

        for (auto it = multicellStatistics.begin(); it != multicellStatistics.end();) {
            auto masterCellTag = it->first;
            if (!secondaryCellTags.contains(masterCellTag) && primaryCellTag != masterCellTag) {
                YT_VERIFY(allowMasterCellRemoval);
                multicellStatistics.erase(it++);
            } else {
                ++it;
            }
        }

        SetLocalPtr(&multicellStatistics[cellTag]);
    }
}

template <class TValue>
void TGossipValue<TValue>::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Cluster_);
    Persist(context, Multicell_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
