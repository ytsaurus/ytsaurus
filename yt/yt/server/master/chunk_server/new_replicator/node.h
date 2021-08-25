#pragma once

#include "public.h"

namespace NYT::NChunkServer::NReplicator {

////////////////////////////////////////////////////////////////////////////////

struct TNode
{
    DEFINE_BYVAL_RO_PROPERTY(TNodeId, Id);
    DEFINE_BYVAL_RO_PROPERTY(TString, DefaultAddress);
};

////////////////////////////////////////////////////////////////////////////////

struct TNodePtrAddressFormatter
{
    void operator()(TStringBuilderBase* builder, TNode* node) const;
};

struct TNodePtrWithIndexesAddressFormatter
{
    void operator()(TStringBuilderBase* builder, TNodePtrWithIndexes node) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer::NReplicator
