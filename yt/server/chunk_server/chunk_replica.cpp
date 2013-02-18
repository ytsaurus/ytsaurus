#include "stdafx.h"
#include "chunk_replica.h"
#include "node.h"

#include <server/cell_master/serialization_context.h>

namespace NYT {
namespace NChunkServer {

using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

#ifdef __x86_64__ 

TChunkReplica::TChunkReplica()
    : Value(0)
{ }

TChunkReplica::TChunkReplica(TDataNode* node, int index)
    : Value(reinterpret_cast<ui64>(node))
{
    YASSERT(reinterpret_cast<ui64>(node) & 0xf000000000000000LL == 0);
    YASSERT(index >= 0 && index < 16);
}

TDataNode* TChunkReplica::GetNode() const
{
    return reinterpret_cast<TDataNode*>(Value & 0x0fffffffffffffffLL);
}

int TChunkReplica::GetIndex() const
{
    return Value >> 60;
}

size_t TChunkReplica::GetHash() const
{
    return static_cast<size_t>(Value);
}

bool TChunkReplica::operator == (TChunkReplica other) const
{
    return Value == other.Value;
}

bool TChunkReplica::operator != (TChunkReplica other) const
{
    return Value != other.Value;
}

#else

TChunkReplica::TChunkReplica()
    : Node(nullptr)
    , Index(0)
{ }

TChunkReplica::TChunkReplica(TDataNode* node, int index)
    : Node(node)
    , Index(index)
{ }

TDataNode* TChunkReplica::GetNode() const
{
    return Node;
}

int TChunkReplica::GetIndex() const
{
    return Index;
}

size_t TChunkReplica::GetHash() const
{
    return THash<TDataNode*>()(Node) * 497 +
           THash<int>()(Index);
}

bool TChunkReplica::operator == (TChunkReplica other) const
{
    return Node == other.Node && Index == other.Index;
}

bool TChunkReplica::operator != (TChunkReplica other) const
{
    return Node != other.Node || Index != other.Index;
}

#endif

bool TChunkReplica::operator < (TChunkReplica other) const
{
    auto thisId = GetNode()->GetId();
    auto otherId = other.GetNode()->GetId();
    if (thisId != otherId) {
        return thisId < otherId;
    }
    return GetIndex() < other.GetIndex();
}

bool TChunkReplica::operator <= (TChunkReplica other) const
{
    auto thisId = GetNode()->GetId();
    auto otherId = other.GetNode()->GetId();
    if (thisId != otherId) {
        return thisId < otherId;
    }
    return GetIndex() <= other.GetIndex();
}

bool TChunkReplica::operator > (TChunkReplica other) const
{
    return other < *this;
}

bool TChunkReplica::operator >= (TChunkReplica other) const
{
    return other <= *this;
}

void SaveObjectRef(TOutputStream* output, TChunkReplica value)
{
    NCellMaster::SaveObjectRef(output, value.GetNode());
    Save(output, value.GetIndex());
}

void LoadObjectRef(TInputStream* input, TChunkReplica& value, const NCellMaster::TLoadContext& context)
{
    TDataNode* node;
    LoadObjectRef(input, node, context);

    int index;
    // COMPAT(babenko)
    if (context.GetVersion() >= 8) {
        Load(input, index);
    } else {
        index = 0;
    }

    value = TChunkReplica(node, index);
}

bool CompareObjectsForSerialization(TChunkReplica lhs, TChunkReplica rhs)
{
    return lhs < rhs;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
