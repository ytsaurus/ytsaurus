#pragma once

#include "public.h"

#include <util/generic/hash.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TGossipValue
{
public:
    using TMulticellValue = THashMap<NObjectClient::TCellTag, TValue>;
    DEFINE_BYREF_RW_PROPERTY(TValue, Cluster);
    DEFINE_BYREF_RW_PROPERTY(TMulticellValue, Multicell);
    DEFINE_BYVAL_RW_PROPERTY(TValue*, LocalPtr);
    DECLARE_BYREF_RW_PROPERTY(TValue, Local);

public:
    TGossipValue();

    TValue* Remote(NObjectClient::TCellTag cellTag);
    void Initialize(const TBootstrap* bootstrap);

    void Persist(TPersistenceContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster

#define GOSSIP_VALUE_INL_H_
#include "gossip_value-inl.h"
#undef GOSSIP_VALUE_INL_H_
