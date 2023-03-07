#pragma once
#ifndef GOSSIP_VALUE_INL_H_
#error "Direct inclusion of this file is not allowed, include gossip_value.h"
// For the sake of sane code completion.
#include "gossip_value.h"
#endif

#include "bootstrap.h"

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
TGossipValue<TValue>::TGossipValue() = default;

template <class TValue>
TValue& TGossipValue<TValue>::Local()
{
    return *LocalPtr_;
}

template <class TValue>
const TValue& TGossipValue<TValue>::Local() const
{
    return *LocalPtr_;
}

template <class TValue>
TValue* TGossipValue<TValue>::Remote(NObjectClient::TCellTag cellTag)
{
    return &GetOrCrash(Multicell(), cellTag);
}

template <class TValue>
void TGossipValue<TValue>::Initialize(const TBootstrap* bootstrap)
{
    auto cellTag = bootstrap->GetCellTag();
    const auto& secondaryCellTags = bootstrap->GetSecondaryCellTags();

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

        SetLocalPtr(&multicellStatistics[cellTag]);
    }
}

template <class TValue>
void TGossipValue<TValue>::Persist(TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Cluster_);
    Persist(context, Multicell_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
