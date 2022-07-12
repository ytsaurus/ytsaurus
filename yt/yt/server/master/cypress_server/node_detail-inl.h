#ifndef NODE_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include node_detail.h"
// For the sake of sane code completion.
#include "node_detail.h"
#endif

#include <yt/yt/server/master/chaos_server/chaos_cell_bundle.h>

#include <yt/yt/server/master/tablet_server/tablet_cell_bundle.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

template <bool Transient>
void TCompositeNodeBase::TGenericAttributes<Transient>::Persist(const NCellMaster::TPersistenceContext& context)
{
    using NCellMaster::EMasterReign;
    using NYT::Persist;

    Persist(context, CompressionCodec);
    Persist(context, ErasureCodec);
    if (context.GetVersion() >= EMasterReign::HunkErasureCodec) {
        Persist(context, HunkErasureCodec);
    }
    if (context.GetVersion() >= EMasterReign::EnableStripedErasureAttribute) {
        Persist(context, EnableStripedErasure);
    }
    Persist(context, ReplicationFactor);
    Persist(context, Vital);
    Persist(context, Atomicity);
    Persist(context, CommitOrdering);
    Persist(context, InMemoryMode);
    Persist(context, OptimizeFor);
    Persist(context, ProfilingMode);
    Persist(context, ProfilingTag);
    Persist(context, ChunkMergerMode);
    Persist(context, PrimaryMediumIndex);
    Persist(context, Media);
    Persist(context, TabletCellBundle);
    if (context.GetVersion() >= EMasterReign::AutoCreateReplicationCard) {
        Persist(context, ChaosCellBundle);
    }
}

template <bool Transient>
void TCompositeNodeBase::TGenericAttributes<Transient>::Persist(const NCypressServer::TCopyPersistenceContext& context)
{
    using NYT::Persist;
#define XX(camelCaseName, snakeCaseName) \
    Persist(context, camelCaseName);
    FOR_EACH_INHERITABLE_ATTRIBUTE(XX);
#undef XX
}

template <bool Transient>
bool TCompositeNodeBase::TGenericAttributes<Transient>::AreFull() const
{
#define XX(camelCaseName, snakeCaseName) \
    && camelCaseName.IsSet()
    return true FOR_EACH_INHERITABLE_ATTRIBUTE(XX);
#undef XX
}

template <bool Transient>
bool TCompositeNodeBase::TGenericAttributes<Transient>::AreEmpty() const
{
#define XX(camelCaseName, snakeCaseName) \
    && !camelCaseName.IsNull()
    return true FOR_EACH_INHERITABLE_ATTRIBUTE(XX);
#undef XX
}

template <bool Transient>
TCompositeNodeBase::TAttributes TCompositeNodeBase::TGenericAttributes<Transient>::ToPersistent() const requires Transient
{
    TAttributes result;
#define XX(camelCaseName, snakeCaseName) \
    if (camelCaseName.IsSet()) { \
        result.camelCaseName.Set(TVersionedBuiltinAttributeTraits<decltype(result.camelCaseName)::TValue>::FromRaw(camelCaseName.Unbox())); \
    }
    FOR_EACH_INHERITABLE_ATTRIBUTE(XX)
#undef XX
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
