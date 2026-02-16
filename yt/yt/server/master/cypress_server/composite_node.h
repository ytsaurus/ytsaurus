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

// process(field, attributeKey) should accept name of
// TCompositeCypressNode::TAttributes field and attribute name in camel case.

#define FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE(process) \
    process(EnableStripedErasure, EnableStripedErasure) \
    process(HunkErasureCodec, HunkErasureCodec) \
    process(Vital, Vital) \
    process(Atomicity, Atomicity) \
    process(CommitOrdering, CommitOrdering) \
    process(InMemoryMode, InMemoryMode) \
    process(OptimizeFor, OptimizeFor) \
    process(ProfilingMode, ProfilingMode) \
    process(ProfilingTag, ProfilingTag) \

#define FOR_EACH_COMPLICATED_INHERITABLE_ATTRIBUTE(process) \
    process(CompressionCodec, CompressionCodec) \
    process(ErasureCodec, ErasureCodec) \
    process(TabletCellBundle, TabletCellBundle) \
    process(ChaosCellBundle, ChaosCellBundle) \
    process(Media, Media) \
    process(HunkMedia, HunkMedia) \
    process(PrimaryMediumIndex, PrimaryMedium) \
    process(HunkPrimaryMediumIndex, HunkPrimaryMedium) \
    process(ReplicationFactor, ReplicationFactor) \
    process(ChunkMergerMode, ChunkMergerMode)

#define FOR_EACH_INHERITABLE_ATTRIBUTE(process) \
    FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE(process) \
    FOR_EACH_COMPLICATED_INHERITABLE_ATTRIBUTE(process)

// NB: For now only chunk_merger_mode will be supported as an inherited attribute in copy, because for others the semantics are non-trivial.
#define FOR_EACH_INHERITABLE_DURING_COPY_ATTRIBUTE(process) \
    process(ChunkMergerMode, ChunkMergerMode)

// Some attributes contain renamable objects. To support safe cross-cell copy
// for such attributes
//   - during attribute set object ID can be used instead of name;
//   - for each such attribute, a transferable alias exists containing object ID
//     instead of object name.
// Example:
//   $ yt get //a/@account
//   my-account
//   $ yt get //a/@account_id
//   #1-2-3-4
//   $ yt set //a/@account my-account  # works
//   $ yt set //a/@account "#1-2-3-4"  # also works
//
// NB: be careful when adding new transferable attribute alias. It's attribute
// author's responsibility to ensure that object name cannot start with "#".

// process(FieldName, AttributeKey, TransferableAliasKey)
#define FOR_EACH_INHERITABLE_ATTRIBUTE_TRANSFERABLE_ALIAS(process) \
    process(TabletCellBundle, TabletCellBundle, TabletCellBundleId) \
    process(ChaosCellBundle, ChaosCellBundle, ChaosCellBundleId) \
    process(PrimaryMediumIndex, PrimaryMedium, PrimaryMediumId) \
    process(HunkPrimaryMediumIndex, HunkPrimaryMedium, HunkPrimaryMediumId) \
    process(Media, Media, TransferableMedia) \
    process(HunkMedia, HunkMedia, TransferableHunkMedia)

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

#define XX(FieldName, AttributeKey) \
public: \
    using T##FieldName = decltype(std::declval<TPersistentAttributes>().FieldName)::TValue; \
    std::optional<TRawVersionedBuiltinAttributeType<T##FieldName>> TryGet##FieldName() const; \
    bool Has##FieldName() const; \
    void Remove##FieldName(); \
    void Set##FieldName(T##FieldName value); \
\
private: \
    const decltype(std::declval<TPersistentAttributes>().FieldName)* DoTryGet##FieldName() const;

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
//! If a setter for a non-inheritable attribute is called, falls back to an
//! ephemeral dictionary.
class TInheritedAttributeDictionary
    : public NYTree::IAttributeDictionary
{
public:
    explicit TInheritedAttributeDictionary(NCellMaster::TBootstrap* bootstrap);
    explicit TInheritedAttributeDictionary(const TConstInheritedAttributeDictionaryPtr& other);
    TInheritedAttributeDictionary(
        const NCellMaster::TBootstrap* bootstrap,
        NYTree::IAttributeDictionaryPtr&& attributes);

    std::vector<TKey> ListKeys() const override;
    std::vector<TKeyValuePair> ListPairs() const override;
    TValue FindYson(TKeyView key) const override;
    void SetYson(TKeyView key, const TValue& value) override;
    bool Remove(TKeyView key) override;

    TCompositeCypressNode::TTransientAttributes& MutableAttributes();
    const TCompositeCypressNode::TTransientAttributes& Attributes() const;

    TInheritedAttributeDictionaryPtr ToNonTransferableView() const;

private:
    const NCellMaster::TBootstrap* Bootstrap_;
    bool Transferable_ = true;
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
