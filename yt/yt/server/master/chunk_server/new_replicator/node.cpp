#include "node.h"

#include <yt/yt/server/master/chunk_server/chunk_replica.h>

namespace NYT::NChunkServer::NReplicator {

////////////////////////////////////////////////////////////////////////////////

void TNodePtrAddressFormatter::operator()(TStringBuilderBase* builder, TNode* node) const
{
    builder->AppendString(node->GetDefaultAddress());
}

void TNodePtrWithIndexesAddressFormatter::operator()(TStringBuilderBase* builder, TNodePtrWithIndexes node) const
{
    builder->AppendString(node.GetPtr()->GetDefaultAddress());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer::NReplicator
