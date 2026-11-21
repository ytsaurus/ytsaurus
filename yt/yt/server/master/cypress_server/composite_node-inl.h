#ifndef COMPOSITE_NODE_INL_H_
#error "Direct inclusion of this file is not allowed, include composite_node.h"
// For the sake of sane code completion.
#include "composite_node.h"
#endif

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

template <bool Transient>
void TCompositeCypressNode::TAttributes<Transient>::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, CompressionCodec);
    PHOENIX_REGISTER_FIELD(2, ErasureCodec);
    PHOENIX_REGISTER_FIELD(3, HunkErasureCodec);
    PHOENIX_REGISTER_FIELD(4, EnableStripedErasure);
    PHOENIX_REGISTER_FIELD(5, ReplicationFactor);
    PHOENIX_REGISTER_FIELD(6, Vital);
    PHOENIX_REGISTER_FIELD(7, Atomicity);
    PHOENIX_REGISTER_FIELD(8, CommitOrdering);
    PHOENIX_REGISTER_FIELD(9, InMemoryMode);
    // COMPAT(cherepashka)
    registrar.template VirtualField<10>("OptimizeFor", [] (TThis* this_, auto& context) {
        this_->LoadOptimizeFor(context);
    }, [] (const TThis* this_, auto& context) {
        NYT::Save(context, this_->OptimizeFor);
    })();
    PHOENIX_REGISTER_FIELD(11, ProfilingMode);
    PHOENIX_REGISTER_FIELD(12, ProfilingTag);
    PHOENIX_REGISTER_FIELD(13, ChunkMergerMode);
    PHOENIX_REGISTER_FIELD(14, PrimaryMediumIndex);
    PHOENIX_REGISTER_FIELD(15, Media);
    PHOENIX_REGISTER_FIELD(16, TabletCellBundle);
    PHOENIX_REGISTER_FIELD(17, ChaosCellBundle);
    PHOENIX_REGISTER_FIELD(18, HunkMedia);
    PHOENIX_REGISTER_FIELD(19, HunkPrimaryMediumIndex);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
