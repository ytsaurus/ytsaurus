#pragma once

#include "public.h"
#include "node.h"

#include <yt/yt/server/lib/tablet_node/public.h>

#include <yt/yt/server/master/chunk_server/chunk_requisition.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ENodeMaterializationReason,
    ((Create)  (0))
    ((Copy)    (1))
);

////////////////////////////////////////////////////////////////////////////////

// NB: The list of inheritable attributes doesn't include the "account"
// attribute because that's already present on every Cypress node.

// NB: Although both Vital and ReplicationFactor can be deduced from Media, it's
// important to be able to specify just the ReplicationFactor (or the Vital
// flag) while leaving Media null.

#define FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE(process) \
    process(CompressionCodec, compression_codec) \
    process(ErasureCodec, erasure_codec) \
    process(EnableStripedErasure, enable_striped_erasure) \
    process(HunkErasureCodec, hunk_erasure_codec) \
    process(ReplicationFactor, replication_factor) \
    process(Vital, vital) \
    process(Atomicity, atomicity) \
    process(CommitOrdering, commit_ordering) \
    process(InMemoryMode, in_memory_mode) \
    process(OptimizeFor, optimize_for) \
    process(ProfilingMode, profiling_mode) \
    process(ProfilingTag, profiling_tag) \
    process(ChunkMergerMode, chunk_merger_mode)

#define FOR_EACH_INHERITABLE_ATTRIBUTE(process) \
    FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE(process) \
    process(TabletCellBundle, tablet_cell_bundle) \
    process(ChaosCellBundle, chaos_cell_bundle) \
    process(PrimaryMediumIndex, primary_medium) \
    process(Media, media) \
    process(HunkPrimaryMediumIndex, hunk_primary_medium) \
    process(HunkMedia, hunk_media)

// NB: For now only chunk_merger_mode will be supported as an inherited attribute in copy, because for others the semantics are non-trivial.
#define FOR_EACH_INHERITABLE_DURING_COPY_ATTRIBUTE(process) \
    process(ChunkMergerMode, chunk_merger_mode)

////////////////////////////////////////////////////////////////////////////////

class TCompositeCypressNode
    : public TCypressNode
{
public:
    //! Contains all nodes with parent pointing here.
    //! When a node dies parent pointers of its immediate descendants are reset.
    DEFINE_BYREF_RW_PROPERTY(THashSet<TCypressNode*>, ImmediateDescendants);

public:
    using TCypressNode::TCypressNode;

    void Save(NCellMaster::TSaveContext& context) const override;
    void Load(NCellMaster::TLoadContext& context) override;

    bool HasInheritableAttributes() const;

private:
    template <bool Transient>
    struct TAttributes
    {
        template <class T>
        using TPtr = std::conditional_t<
            Transient,
            NObjectServer::TRawObjectPtr<T>,
            NObjectServer::TStrongObjectPtr<T>
        >;

        TVersionedBuiltinAttribute<NCompression::ECodec> CompressionCodec;
        TVersionedBuiltinAttribute<NErasure::ECodec> ErasureCodec;
        TVersionedBuiltinAttribute<NErasure::ECodec> HunkErasureCodec;
        TVersionedBuiltinAttribute<bool> EnableStripedErasure;
        TVersionedBuiltinAttribute<int> PrimaryMediumIndex;
        TVersionedBuiltinAttribute<int> HunkPrimaryMediumIndex;
        TVersionedBuiltinAttribute<NChunkServer::TChunkReplication> Media;
        TVersionedBuiltinAttribute<NChunkServer::TChunkReplication> HunkMedia;
        TVersionedBuiltinAttribute<int> ReplicationFactor;
        TVersionedBuiltinAttribute<bool> Vital;
        TVersionedBuiltinAttribute<TPtr<NTabletServer::TTabletCellBundle>> TabletCellBundle;
        TVersionedBuiltinAttribute<TPtr<NChaosServer::TChaosCellBundle>> ChaosCellBundle;
        TVersionedBuiltinAttribute<NTransactionClient::EAtomicity> Atomicity;
        TVersionedBuiltinAttribute<NTransactionClient::ECommitOrdering> CommitOrdering;
        TVersionedBuiltinAttribute<NTabletClient::EInMemoryMode> InMemoryMode;
        TVersionedBuiltinAttribute<NTableClient::EOptimizeFor> OptimizeFor;
        TVersionedBuiltinAttribute<NTabletNode::EDynamicTableProfilingMode> ProfilingMode;
        TVersionedBuiltinAttribute<std::string> ProfilingTag;
        TVersionedBuiltinAttribute<NChunkClient::EChunkMergerMode> ChunkMergerMode;

        void Persist(const NCellMaster::TPersistenceContext& context) requires (!Transient);

        // NB: on source cell attributes are serialized as transient to avoid references.
        // On destination cell attributes are changed to persistent using ToPersist method.
        void Persist(const NCypressServer::TCopyPersistenceContext& context) requires Transient;

        // Are all attributes not null?
        bool AreFull() const;

        // Are all attributes null?
        bool AreEmpty() const;

        TAttributes<false> ToPersistent() const requires Transient;

        // CClonable.
        TAttributes Clone() const requires (!Transient);
    };

public:
    using TTransientAttributes = TAttributes</*Transient*/ true>;
    using TPersistentAttributes = TAttributes</*Transient*/ false>;

    virtual bool CompareInheritableAttributes(
        const TTransientAttributes& attributes,
        ENodeMaterializationReason reason = ENodeMaterializationReason::Create) const;

    virtual TConstInheritedAttributeDictionaryPtr MaybePatchInheritableAttributes(
        const TConstInheritedAttributeDictionaryPtr& attributes,
        ENodeMaterializationReason reason = ENodeMaterializationReason::Create) const;

    virtual void FillInheritableAttributes(
        TTransientAttributes* attributes,
        ENodeMaterializationReason reason = ENodeMaterializationReason::Create) const;

#define XX(camelCaseName, snakeCaseName) \
public: \
    using T##camelCaseName = decltype(std::declval<TPersistentAttributes>().camelCaseName)::TValue; \
    std::optional<TRawVersionedBuiltinAttributeType<T##camelCaseName>> TryGet##camelCaseName() const; \
    bool Has##camelCaseName() const; \
    void Remove##camelCaseName(); \
    void Set##camelCaseName(T##camelCaseName value); \
\
private: \
    const decltype(std::declval<TPersistentAttributes>().camelCaseName)* DoTryGet##camelCaseName() const;

    FOR_EACH_INHERITABLE_ATTRIBUTE(XX)
#undef XX

    template <class U>
    friend class TCompositeCypressNodeTypeHandlerBase;

public:
    // TODO(kvk1920): Consider accessing Attributes_ via type handler.
    const TPersistentAttributes* FindAttributes() const;

private:
    void SetAttributes(const TPersistentAttributes* attributes);
    void CloneAttributesFrom(const TCompositeCypressNode* sourceNode);
    void MergeAttributesFrom(const TCompositeCypressNode* branchedNode);

    std::unique_ptr<TPersistentAttributes> Attributes_;
};

DEFINE_MASTER_OBJECT_TYPE(TCompositeCypressNode)

////////////////////////////////////////////////////////////////////////////////

//! A set of inheritable attributes represented as an attribute dictionary.
//! If a setter for a non-inheritable attribute is called, falls back to an ephemeral dictionary.
class TInheritedAttributeDictionary
    : public NYTree::IAttributeDictionary
{
public:
    explicit TInheritedAttributeDictionary(NCellMaster::TBootstrap* bootstrap);
    explicit TInheritedAttributeDictionary(const TConstInheritedAttributeDictionaryPtr& other);
    explicit TInheritedAttributeDictionary(
        const NCellMaster::TBootstrap* bootstrap,
        NYTree::IAttributeDictionaryPtr&& attributes);

    std::vector<TKey> ListKeys() const override;
    std::vector<TKeyValuePair> ListPairs() const override;
    TValue FindYson(TKeyView key) const override;
    void SetYson(TKeyView key, const TValue& value) override;
    bool Remove(TKeyView key) override;

    TCompositeCypressNode::TTransientAttributes& MutableAttributes();
    const TCompositeCypressNode::TTransientAttributes& Attributes() const;

private:
    const NCellMaster::TBootstrap* Bootstrap_;
    TCompositeCypressNode::TTransientAttributes InheritedAttributes_;
    NYTree::IAttributeDictionaryPtr Fallback_;
};

////////////////////////////////////////////////////////////////////////////////

// Traverse all ancestors and collect inheritable attributes.
void GatherInheritableAttributes(
    const TCypressNode* node,
    TCompositeCypressNode::TTransientAttributes* attributes,
    ENodeMaterializationReason reason = ENodeMaterializationReason::Create);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
