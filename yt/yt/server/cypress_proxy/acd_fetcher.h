#pragma once

#include "public.h"

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

// The class is designed to fetch and cache Access Control Descriptors for nodes in
// a Cypress tree. Since the ACD of an object may be requested multiple times during
// the common permission validation pipeline, caching this information in the context
// of an execution session allows to avoid redundant lookups. Also, it handles trivial
// ACDs for objects with pending records at GUQM.
class TAcdFetcher
    : public TRefCounted
{
public:
    explicit TAcdFetcher(NSequoiaClient::ISequoiaTransactionPtr sequoiaTransaction);

    std::vector<const TAccessControlDescriptor*> Fetch(
        TRange<TRange<TCypressNodeDescriptor>> joinedDescriptors);

private:
    const NSequoiaClient::ISequoiaTransactionPtr SequoiaTransaction_;

    THashMap<NCypressClient::TNodeId, TAccessControlDescriptor> NodeIdToAcd_;

    static TAccessControlDescriptor MakeDefaultDescriptor(NCypressClient::TNodeId nodeId);
};

DEFINE_REFCOUNTED_TYPE(TAcdFetcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
