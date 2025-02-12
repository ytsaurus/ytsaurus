#pragma once

#include "cypress_manager.h"
#include "node.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/chunk_server/chunk_requisition.h>

#include <yt/yt/server/master/object_server/attribute_set.h>
#include <yt/yt/server/master/object_server/object_detail.h>
#include <yt/yt/server/master/object_server/object_part_cow_ptr.h>
#include <yt/yt/server/master/object_server/type_handler_detail.h>

#include <yt/yt/server/master/security_server/account.h>
#include <yt/yt/server/master/security_server/detailed_master_memory.h>
#include <yt/yt/server/master/security_server/security_manager.h>

#include <yt/yt/server/master/tablet_server/tablet_cell_bundle.h>

#include <yt/yt/server/lib/tablet_node/public.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/sequoia_client/transaction.h>

#include <yt/yt/ytlib/sequoia_client/records/path_to_node_id.record.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/node_detail.h>
#include <yt/yt/core/ytree/overlaid_attribute_dictionaries.h>
#include <yt/yt/core/ytree/tree_builder.h>
#include <yt/yt_proto/yt/core/ytree/proto/ypath.pb.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TNontemplateCypressNodeTypeHandlerBase
    : public INodeTypeHandler
{
public:
    explicit TNontemplateCypressNodeTypeHandlerBase(NCellMaster::TBootstrap* bootstrap);

    NObjectServer::ETypeFlags GetFlags() const override;

    void FillAttributes(
        TCypressNode* trunkNode,
        NYTree::IAttributeDictionary* inheritedAttributes,
        NYTree::IAttributeDictionary* explicitAttributes) override;

    virtual bool IsSupportedInheritableAttribute(const std::string& /*key*/) const;

    NObjectServer::TAcdList ListAcds(TCypressNode* trunkNode) const override;

protected:
    NObjectServer::TObjectTypeMetadata Metadata_;

    void SetBootstrap(NCellMaster::TBootstrap* bootstrap);
    NCellMaster::TBootstrap* GetBootstrap() const;

    bool IsLeader() const;
    bool IsRecovery() const;
    const TDynamicCypressManagerConfigPtr& GetDynamicCypressManagerConfig() const;

    void DestroyCorePrologue(TCypressNode* node);

    void SerializeNodeCore(
        TCypressNode* node,
        TSerializeNodeContext* context);
    TCypressNode* MaterializeNodeCore(
        TMaterializeNodeContext* context,
        ICypressNodeFactory* factory);

    void BranchCorePrologue(
        TCypressNode* originatingNode,
        TCypressNode* branchedNode,
        NTransactionServer::TTransaction* transaction,
        const TLockRequest& lockRequest);
    void BranchCoreEpilogue(
        TCypressNode* branchedNode);

    void MergeCorePrologue(
        TCypressNode* originatingNode,
        TCypressNode* branchedNode);
    void MergeCoreEpilogue(
        TCypressNode* originatingNode,
        TCypressNode* branchedNode);

    TCypressNode* CloneCorePrologue(
        ICypressNodeFactory* factory,
        TNodeId hintId,
        TCypressNode* sourceNode,
        NSecurityServer::TAccount* account);
    void CloneCoreEpilogue(
        TCypressNode* sourceNode,
        TCypressNode* clonedTrunkNode,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode);

private:
    NCellMaster::TBootstrap* Bootstrap_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TCypressNodeTypeHandlerBase
    : public TNontemplateCypressNodeTypeHandlerBase
{
public:
    TCypressNodeTypeHandlerBase()
        : TNontemplateCypressNodeTypeHandlerBase(/*bootstrap*/ nullptr)
    { }

    explicit TCypressNodeTypeHandlerBase(NCellMaster::TBootstrap* bootstrap)
        : TNontemplateCypressNodeTypeHandlerBase(bootstrap)
    { }

    ICypressNodeProxyPtr GetProxy(
        TCypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction) override
    {
        return DoGetProxy(trunkNode->As<TImpl>(), transaction);
    }

    NTableServer::TMasterTableSchema* FindSchema(TCypressNode* node) const override
    {
        return DoFindSchema(node->As<TImpl>());
    }

    i64 GetStaticMasterMemoryUsage() const override
    {
        return sizeof(TImpl);
    }

    std::unique_ptr<TCypressNode> Instantiate(
        TVersionedNodeId id,
        NObjectClient::TCellTag externalCellTag) override
    {
        auto nodeHolder = std::unique_ptr<TCypressNode>(TPoolAllocator::New<TImpl>(id));
        nodeHolder->SetExternalCellTag(externalCellTag);
        nodeHolder->SetTrunkNode(nodeHolder.get());

        const auto& multicellManager = GetBootstrap()->GetMulticellManager();
        if (nodeHolder->GetNativeCellTag() != multicellManager->GetCellTag()) {
            nodeHolder->SetForeign();
        }

        return nodeHolder;
    }

    std::unique_ptr<TCypressNode> Create(
        TNodeId hintId,
        const TCreateNodeContext& context) override
    {
        const auto& objectManager = GetBootstrap()->GetObjectManager();
        auto id = objectManager->GenerateId(GetObjectType(), hintId);
        return DoCreate(TVersionedNodeId(id), context);
    }

    void SetReachable(TCypressNode* node) override
    {
        YT_VERIFY(!node->GetReachable());
        node->SetReachable(true);

        auto* typedNode = node->As<TImpl>();
        DoSetReachable(typedNode);
    }

    void SetUnreachable(TCypressNode* node) override
    {
        YT_VERIFY(node->GetReachable());
        node->SetReachable(false);

        auto* typedNode = node->As<TImpl>();
        DoSetUnreachable(typedNode);
    }

    void Zombify(TCypressNode* node) override
    {
        auto* typedNode = node->As<TImpl>();
        DoZombify(typedNode);
    }

    void Destroy(TCypressNode* node) override
    {
        // Run core stuff.
        DestroyCorePrologue(node);

        // Run custom stuff.
        auto* typedNode = node->As<TImpl>();
        DoDestroy(typedNode);
    }

    void DestroySequoiaObject(
        TCypressNode* node,
        const NSequoiaClient::ISequoiaTransactionPtr& transaction) noexcept override
    {
        auto* typedNode = node->As<TImpl>();
        DoDestroySequoiaObject(typedNode, transaction);
    }

    void RecreateAsGhost(TCypressNode* node) override
    {
        auto* typedNode = node->As<TImpl>();
        NObjectServer::TObject::RecreateAsGhost(typedNode);
    }

    void SerializeNode(
        TCypressNode* node,
        TSerializeNodeContext* context) override
    {
        SerializeNodeCore(node, context);
        DoSerializeNode(node->As<TImpl>(), context);
    }

    TCypressNode* MaterializeNode(
        TMaterializeNodeContext* context,
        ICypressNodeFactory* factory) override
    {
        auto* trunkNode = MaterializeNodeCore(context, factory);
        DoMaterializeNode(trunkNode->template As<TImpl>(), context);

        return trunkNode;
    }

    std::unique_ptr<TCypressNode> Branch(
        TCypressNode* originatingNode,
        NTransactionServer::TTransaction* transaction,
        const TLockRequest& lockRequest) override
    {
        // Instantiate a branched copy.
        auto originatingId = originatingNode->GetVersionedId();
        auto branchedId = TVersionedNodeId(originatingId.ObjectId, GetObjectId(transaction));
        auto branchedNodeHolder = TPoolAllocator::New<TImpl>(branchedId);
        auto* typedBranchedNode = branchedNodeHolder.get();

        // Run core stuff.
        auto* typedOriginatingNode = originatingNode->As<TImpl>();
        BranchCorePrologue(typedOriginatingNode, typedBranchedNode, transaction, lockRequest);

        // Run custom stuff.
        DoBranch(typedOriginatingNode, typedBranchedNode, lockRequest);
        DoLogBranch(typedOriginatingNode, typedBranchedNode, lockRequest);

        // Run core stuff.
        BranchCoreEpilogue(typedBranchedNode);

        return std::move(branchedNodeHolder);
    }

    void Unbranch(
        TCypressNode* originatingNode,
        TCypressNode* branchedNode) override
    {
        // Run custom stuff.
        auto* typedOriginatingNode = originatingNode->As<TImpl>();
        auto* typedBranchedNode = branchedNode->As<TImpl>();
        DoUnbranch(typedOriginatingNode, typedBranchedNode);
        DoLogUnbranch(typedOriginatingNode, typedBranchedNode);
    }

    void Merge(
        TCypressNode* originatingNode,
        TCypressNode* branchedNode) override
    {
        // Run core stuff.
        auto* typedOriginatingNode = originatingNode->As<TImpl>();
        auto* typedBranchedNode = branchedNode->As<TImpl>();
        MergeCorePrologue(typedOriginatingNode, typedBranchedNode);

        // Run custom stuff.
        DoMerge(typedOriginatingNode, typedBranchedNode);
        DoLogMerge(typedOriginatingNode, typedBranchedNode);

        // Run core stuff.
        MergeCoreEpilogue(typedOriginatingNode, typedBranchedNode);
    }

    TCypressNode* Clone(
        TCypressNode* sourceNode,
        NYTree::IAttributeDictionary* inheritedAttributes,
        ICypressNodeFactory* factory,
        TNodeId hintId,
        ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override
    {
        // Run core prologue stuff.
        auto* clonedTrunkNode = CloneCorePrologue(
            factory,
            hintId,
            sourceNode,
            account);

        // Run custom stuff.
        auto* typedSourceNode = sourceNode->template As<TImpl>();
        auto* typedClonedTrunkNode = clonedTrunkNode->template As<TImpl>();
        DoClone(typedSourceNode, typedClonedTrunkNode, inheritedAttributes, factory, mode, account);

        // Run core epilogue stuff.
        CloneCoreEpilogue(
            sourceNode,
            clonedTrunkNode,
            factory,
            mode);

        return clonedTrunkNode;
    }

    bool HasBranchedChanges(
        TCypressNode* originatingNode,
        TCypressNode* branchedNode) override
    {
        return HasBranchedChangesImpl(
            originatingNode->template As<TImpl>(),
            branchedNode->template As<TImpl>());
    }

    std::optional<std::vector<std::string>> ListColumns(TCypressNode* node) const override
    {
        return DoListColumns(node->template As<TImpl>());
    }

protected:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TImpl* trunkNode,
        NTransactionServer::TTransaction* transaction) = 0;

    virtual NTableServer::TMasterTableSchema* DoFindSchema(TImpl* /*node*/) const
    {
        return nullptr;
    }

    virtual std::unique_ptr<TImpl> DoCreate(
        NCypressServer::TVersionedNodeId id,
        const TCreateNodeContext& context)
    {
        auto nodeHolder = TPoolAllocator::New<TImpl>(id);
        nodeHolder->SetExternalCellTag(context.ExternalCellTag);
        nodeHolder->SetTrunkNode(nodeHolder.get());

        const auto& multicellManager = GetBootstrap()->GetMulticellManager();
        if (nodeHolder->GetNativeCellTag() != multicellManager->GetCellTag()) {
            nodeHolder->SetForeign();
            nodeHolder->SetNativeContentRevision(context.NativeContentRevision);
        }

        const auto& securityManager = GetBootstrap()->GetSecurityManager();
        auto* user = securityManager->GetAuthenticatedUser();
        securityManager->ValidatePermission(context.Account, user, NSecurityServer::EPermission::Use);
        // Null is passed as transaction because DoCreate() always creates trunk nodes.
        securityManager->SetAccount(
            nodeHolder.get(),
            context.Account,
            nullptr /*transaction*/);

        return nodeHolder;
    }

    virtual void DoSetReachable(TImpl* /*node*/)
    { }

    virtual void DoSetUnreachable(TImpl* /*node*/)
    { }

    virtual void DoZombify(TImpl* /*node*/)
    { }

    virtual void DoDestroy(TImpl* /*node*/)
    { }

    virtual void DoDestroySequoiaObject(
        TImpl* /*node*/,
        const NSequoiaClient::ISequoiaTransactionPtr& /*transaction*/) noexcept
    { }

    virtual void DoSerializeNode(
        TImpl* /*node*/,
        TSerializeNodeContext* /*context*/)
    { }

    virtual void DoMaterializeNode(
        TImpl* /*trunkNode*/,
        TMaterializeNodeContext* /*context*/)
    { }

    virtual void DoBranch(
        const TImpl* /*originatingNode*/,
        TImpl* /*branchedNode*/,
        const TLockRequest& /*lockRequest*/)
    { }

    virtual void DoLogBranch(
        const TImpl* originatingNode,
        TImpl* branchedNode,
        const TLockRequest& lockRequest)
    {
        const auto& Logger = CypressServerLogger;
        YT_LOG_DEBUG(
            "Node branched (OriginatingNodeId: %v, BranchedNodeId: %v, Mode: %v, LockTimestamp: %v)",
            originatingNode->GetVersionedId(),
            branchedNode->GetVersionedId(),
            lockRequest.Mode,
            lockRequest.Timestamp);
    }

    virtual void DoMerge(TImpl* /*originatingNode*/, TImpl* /*branchedNode*/)
    {
        // NB: Some subclasses (namely, the journal type handler) don't
        // chain-call base class method. So it's probably not a good idea to put
        // any code here. (Hint: put it in MergeCore{Pro,Epi}logue instead.)
    }

    virtual void DoLogMerge(
        TImpl* originatingNode,
        TImpl* branchedNode)
    {
        const auto& Logger = CypressServerLogger;
        YT_LOG_DEBUG(
            "Node merged (OriginatingNodeId: %v, BranchedNodeId: %v)",
            originatingNode->GetVersionedId(),
            branchedNode->GetVersionedId());
    }

    virtual void DoUnbranch(
        TImpl* /*originatingNode*/,
        TImpl* /*branchedNode*/)
    { }

    virtual void DoLogUnbranch(
        TImpl* /*originatingNode*/,
        TImpl* /*branchedNode*/)
    { }

    virtual void DoClone(
        TImpl* /*sourceNode*/,
        TImpl* /*clonedTrunkNode*/,
        NYTree::IAttributeDictionary* /*inheritedAttributes*/,
        ICypressNodeFactory* /*factory*/,
        ENodeCloneMode /*mode*/,
        NSecurityServer::TAccount* /*account*/)
    { }

    virtual bool HasBranchedChangesImpl(
        TImpl* /*originatingNode*/,
        TImpl* /*branchedNode*/)
    {
        return false;
    }

    virtual std::optional<std::vector<std::string>> DoListColumns(TImpl* /*node*/) const
    {
        return std::nullopt;
    }
};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class TValue>
struct TCypressScalarTypeTraits
{ };

template <>
struct TCypressScalarTypeTraits<TString>
{
    static constexpr NObjectClient::EObjectType ObjectType = NObjectClient::EObjectType::StringNode;
    static constexpr NYTree::ENodeType NodeType = NYTree::ENodeType::String;
};

template <>
struct TCypressScalarTypeTraits<i64>
{
    static constexpr NObjectClient::EObjectType ObjectType = NObjectClient::EObjectType::Int64Node;
    static constexpr NYTree::ENodeType NodeType = NYTree::ENodeType::Int64;
};

template <>
struct TCypressScalarTypeTraits<ui64>
{
    static constexpr NObjectClient::EObjectType ObjectType = NObjectClient::EObjectType::Uint64Node;
    static constexpr NYTree::ENodeType NodeType = NYTree::ENodeType::Uint64;
};

template <>
struct TCypressScalarTypeTraits<double>
{
    static constexpr NObjectClient::EObjectType ObjectType = NObjectClient::EObjectType::DoubleNode;
    static constexpr NYTree::ENodeType NodeType = NYTree::ENodeType::Double;
};

template <>
struct TCypressScalarTypeTraits<bool>
{
    static constexpr NObjectClient::EObjectType ObjectType = NObjectClient::EObjectType::BooleanNode;
    static constexpr NYTree::ENodeType NodeType = NYTree::ENodeType::Boolean;
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TScalarNode
    : public TCypressNode
{
public:
    DEFINE_BYREF_RW_PROPERTY(TValue, Value);

public:
    using TCypressNode::TCypressNode;

    explicit TScalarNode(const TVersionedNodeId& id)
        : TCypressNode(id)
        , Value_()
    { }

    NYTree::ENodeType GetNodeType() const override
    {
        return NDetail::TCypressScalarTypeTraits<TValue>::NodeType;
    }

    void Save(NCellMaster::TSaveContext& context) const override
    {
        TCypressNode::Save(context);

        using NYT::Save;
        Save(context, Value_);
    }

    void Load(NCellMaster::TLoadContext& context) override
    {
        TCypressNode::Load(context);

        using NYT::Load;
        Load(context, Value_);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TScalarNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TScalarNode<TValue>>
{
public:
    explicit TScalarNodeTypeHandler(NCellMaster::TBootstrap* bootstrap)
        : TBase(bootstrap)
    { }

    NObjectClient::EObjectType GetObjectType() const override
    {
        return NDetail::TCypressScalarTypeTraits<TValue>::ObjectType;
    }

    NYTree::ENodeType GetNodeType() const override
    {
        return NDetail::TCypressScalarTypeTraits<TValue>::NodeType;
    }

protected:
    using TBase = TCypressNodeTypeHandlerBase<TScalarNode<TValue>>;

    ICypressNodeProxyPtr DoGetProxy(
        TScalarNode<TValue>* trunkNode,
        NTransactionServer::TTransaction* transaction) override;

    void DoBranch(
        const TScalarNode<TValue>* originatingNode,
        TScalarNode<TValue>* branchedNode,
        const TLockRequest& lockRequest) override
    {
        TBase::DoBranch(originatingNode, branchedNode, lockRequest);

        branchedNode->Value() = originatingNode->Value();
    }

    void DoMerge(
        TScalarNode<TValue>* originatingNode,
        TScalarNode<TValue>* branchedNode) override
    {
        TBase::DoMerge(originatingNode, branchedNode);

        originatingNode->Value() = branchedNode->Value();
    }

    void DoClone(
        TScalarNode<TValue>* sourceNode,
        TScalarNode<TValue>* clonedTrunkNode,
        NYTree::IAttributeDictionary* inheritedAttributes,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override
    {
        TBase::DoClone(sourceNode, clonedTrunkNode, inheritedAttributes, factory, mode, account);

        clonedTrunkNode->Value() = sourceNode->Value();
    }

    void DoSerializeNode(
        TScalarNode<TValue>* node,
        TSerializeNodeContext* context) override
    {
        TBase::DoSerializeNode(node, context);

        using NYT::Save;
        Save(*context, node->Value());
    }

    void DoMaterializeNode(
        TScalarNode<TValue>* trunkNode,
        TMaterializeNodeContext* context) override
    {
        TBase::DoMaterializeNode(trunkNode, context);

        using NYT::Load;
        Load(*context, trunkNode->Value());
    }
};

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

class TInheritedAttributeDictionary;
using TConstInheritedAttributeDictionaryPtr = TIntrusivePtr<const TInheritedAttributeDictionary>;

class TCompositeNodeBase
    : public TCypressNode
{
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
        TVersionedBuiltinAttribute<TString> ProfilingTag;
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
    friend class TCompositeNodeTypeHandler;
public:
    // TODO(kvk1920): Consider accessing Attributes_ via type handler.
    const TPersistentAttributes* FindAttributes() const;

private:
    void SetAttributes(const TPersistentAttributes* attributes);
    void CloneAttributesFrom(const TCompositeNodeBase* sourceNode);
    void MergeAttributesFrom(const TCompositeNodeBase* branchedNode);

    std::unique_ptr<TPersistentAttributes> Attributes_;
};

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

    TCompositeNodeBase::TTransientAttributes& MutableAttributes();
    const TCompositeNodeBase::TTransientAttributes& Attributes() const;

private:
    const NCellMaster::TBootstrap* Bootstrap_;
    TCompositeNodeBase::TTransientAttributes InheritedAttributes_;
    NYTree::IAttributeDictionaryPtr Fallback_;
};

////////////////////////////////////////////////////////////////////////////////

// Traverse all ancestors and collect inheritable attributes.
void GatherInheritableAttributes(
    const TCypressNode* node,
    TCompositeNodeBase::TTransientAttributes* attributes,
    ENodeMaterializationReason reason = ENodeMaterializationReason::Create);

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TCompositeNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TImpl>
{
private:
    using TBase = TCypressNodeTypeHandlerBase<TImpl>;

public:
    using TBase::TBase;

protected:
    void DoClone(
        TImpl* sourceNode,
        TImpl* clonedTrunkNode,
        NYTree::IAttributeDictionary* inheritedAttributes,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override;

    void DoBranch(
        const TImpl* originatingNode,
        TImpl* branchedNode,
        const TLockRequest& lockRequest) override;
    void DoMerge(
        TImpl* originatingNode,
        TImpl* branchedNode) override;
    bool HasBranchedChangesImpl(
        TImpl* originatingNode,
        TImpl* branchedNode) override;

    void DoSerializeNode(
        TImpl* node,
        TSerializeNodeContext* context) override;
    void DoMaterializeNode(
        TImpl* trunkNode,
        TMaterializeNodeContext* context) override;
};

////////////////////////////////////////////////////////////////////////////////

//! The core of a map node. May be shared between multiple map nodes for CoW optimization.
//! Designed to be wrapped into TObjectPartCoWPtr.
// NB: The implementation of this template class can be found in _cpp_file,
// together with all relevant explicit instantiations.
template <class TNonOwnedChild>
class TMapNodeChildren
{
private:
    template <class TNonOwnedChild_>
    struct TChildTraits
    {
        using T = TNonOwnedChild_;
        static constexpr bool IsPointer = false;
    };

    template <class TObject>
    struct TChildTraits<NObjectServer::TRawObjectPtr<TObject>>
    {
        using T = NObjectServer::TStrongObjectPtr<TObject>;
        static constexpr bool IsPointer = true;
    };

    using TMaybeOwnedChild = typename TChildTraits<TNonOwnedChild>::T;

    static TMaybeOwnedChild ToOwnedOnLoad(TNonOwnedChild child);
    static TMaybeOwnedChild Clone(const TMaybeOwnedChild& child);
    static void MaybeVerifyIsTrunk(TNonOwnedChild child);

public:
    static constexpr bool ChildIsPointer = TChildTraits<TNonOwnedChild>::IsPointer;
    using TKeyToChild = THashMap<std::string, TNonOwnedChild, THash<std::string_view>, TEqualTo<std::string_view>>;
    using TChildToKey = THashMap<TMaybeOwnedChild, std::string>;

    static bool IsNull(TNonOwnedChild child) noexcept;

    TMapNodeChildren() = default;

    // Refcounted classes never are - and shouldn't be - copyable.
    TMapNodeChildren(const TMapNodeChildren&) = delete;
    TMapNodeChildren& operator=(const TMapNodeChildren&) = delete;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void RecomputeMasterMemoryUsage();

    void Set(const std::string& key, TNonOwnedChild child);
    void Insert(const std::string& key, TNonOwnedChild child);
    void Remove(const std::string& key, TNonOwnedChild child);
    bool Contains(const std::string& key) const;

    const TKeyToChild& KeyToChild() const;
    const TChildToKey& ChildToKey() const;

    int GetRefCount() const noexcept;
    void Ref() noexcept;
    void Unref() noexcept;

    static std::unique_ptr<TMapNodeChildren> Copy(TMapNodeChildren* srcChildren);

    DEFINE_BYVAL_RO_PROPERTY(i64, MasterMemoryUsage);

private:
    TKeyToChild KeyToChild_;
    TChildToKey ChildToKey_;
    int RefCount_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

// NB: The implementation of this template class can be found in _cpp_file,
// together with all relevant explicit instantiations.
template <class TChild>
class TMapNodeImpl
    : public TCompositeNodeBase
{
public:
    using TChildren = TMapNodeChildren<TChild>;
    using TKeyToChild = typename TChildren::TKeyToChild;
    using TChildToKey = typename TChildren::TChildToKey;

    DEFINE_BYREF_RW_PROPERTY(int, ChildCountDelta);

public:
    using TCompositeNodeBase::TCompositeNodeBase;

    explicit TMapNodeImpl(const TMapNodeImpl&) = delete;
    TMapNodeImpl& operator=(const TMapNodeImpl&) = delete;

    const TKeyToChild& KeyToChild() const;
    const TChildToKey& ChildToKey() const;

    // Potentially does the 'copy' part of CoW.
    TChildren& MutableChildren();

    NYTree::ENodeType GetNodeType() const override;

    void Save(NCellMaster::TSaveContext& context) const override;
    void Load(NCellMaster::TLoadContext& context) override;

    void AssignChildren(const NObjectServer::TObjectPartCoWPtr<TChildren>& children);

    int GetGCWeight() const override;

    NSecurityServer::TDetailedMasterMemory GetDetailedMasterMemoryUsage() const override;

    uintptr_t GetMapNodeChildrenAddress() const;

    static bool IsNull(TChild child) noexcept;

protected:
    NObjectServer::TObjectPartCoWPtr<TChildren> Children_;

    template <class TImpl>
    friend class TCypressMapNodeTypeHandlerImpl;
    template <class TImpl>
    friend class TSequoiaMapNodeTypeHandlerImpl;
};

////////////////////////////////////////////////////////////////////////////////

// NB: The implementation of this template class can be found in _cpp_ file,
// together with all relevant explicit instantiations.
template <class TImpl = TCypressMapNode>
class TCypressMapNodeTypeHandlerImpl
    : public TCompositeNodeTypeHandler<TImpl>
{
public:
    using TBase = TCompositeNodeTypeHandler<TImpl>;

    using TBase::TBase;

    NObjectClient::EObjectType GetObjectType() const override;
    NYTree::ENodeType GetNodeType() const override;

protected:
    ICypressNodeProxyPtr DoGetProxy(
        TImpl* trunkNode,
        NTransactionServer::TTransaction* transaction) override;

    void DoDestroy(TImpl* node) override;

    void DoBranch(
        const TImpl* originatingNode,
        TImpl* branchedNode,
        const TLockRequest& lockRequest) override;

    void DoMerge(
        TImpl* originatingNode,
        TImpl* branchedNode) override;

    void DoClone(
        TImpl* sourceNode,
        TImpl* clonedTrunkNode,
        NYTree::IAttributeDictionary* inheritedAttributes,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override;

    bool HasBranchedChangesImpl(
        TImpl* originatingNode,
        TImpl* branchedNode) override;

    void DoSerializeNode(
        TImpl* node,
        TSerializeNodeContext* context) override;
    void DoMaterializeNode(
        TImpl* trunkNode,
        TMaterializeNodeContext* context) override;
};

using TCypressMapNodeTypeHandler = TCypressMapNodeTypeHandlerImpl<TCypressMapNode>;

////////////////////////////////////////////////////////////////////////////////

// TODO(kvk1920): Try to make some common base for Cypress and Sequoia map nodes.
// (E.g. branching is similar in both cases).
template <class TImpl = TSequoiaMapNode>
class TSequoiaMapNodeTypeHandlerImpl
    : public TCypressNodeTypeHandlerBase<TImpl>
{
public:
    using TBase = TCypressNodeTypeHandlerBase<TImpl>;

    using TBase::TBase;

    NObjectClient::EObjectType GetObjectType() const override;
    NYTree::ENodeType GetNodeType() const override;

protected:
    ICypressNodeProxyPtr DoGetProxy(
        TImpl* trunkNode,
        NTransactionServer::TTransaction* transaction) override;

    void DoDestroy(TImpl* node) override;

    void DoBranch(
        const TImpl* originatingNode,
        TImpl* branchedNode,
        const TLockRequest& lockRequest) override;

    void DoMerge(
        TImpl* originatingNode,
        TImpl* branchedNode) override;

    void DoClone(
        TImpl* sourceNode,
        TImpl* clonedTrunkNode,
        NYTree::IAttributeDictionary* inheritedAttributes,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override;

    bool HasBranchedChangesImpl(
        TImpl* originatingNode,
        TImpl* branchedNode) override;

    void DoSerializeNode(
        TImpl* node,
        TSerializeNodeContext* context) override;
    void DoMaterializeNode(
        TImpl* trunkNode,
        TMaterializeNodeContext* context) override;
};

using TSequoiaMapNodeTypeHandler = TSequoiaMapNodeTypeHandlerImpl<TSequoiaMapNode>;

////////////////////////////////////////////////////////////////////////////////

class TListNode
    : public TCompositeNodeBase
{
private:
    using TBase = TCompositeNodeBase;

public:
    using TIndexToChild = std::vector<TCypressNodeRawPtr>;
    using TChildToIndex = THashMap<TCypressNodeRawPtr, int>;

    DEFINE_BYREF_RW_PROPERTY(TIndexToChild, IndexToChild);
    DEFINE_BYREF_RW_PROPERTY(TChildToIndex, ChildToIndex);

public:
    using TBase::TBase;

    NYTree::ENodeType GetNodeType() const override;

    void Save(NCellMaster::TSaveContext& context) const override;
    void Load(NCellMaster::TLoadContext& context) override;

    int GetGCWeight() const override;
};

////////////////////////////////////////////////////////////////////////////////

class TListNodeTypeHandler
    : public TCompositeNodeTypeHandler<TListNode>
{
private:
    using TBase = TCompositeNodeTypeHandler<TListNode>;

public:
    using TBase::TBase;

    NObjectClient::EObjectType GetObjectType() const override;
    NYTree::ENodeType GetNodeType() const override;

    std::unique_ptr<TCypressNode> Create(
        TNodeId hintId,
        const TCreateNodeContext& context) override;

private:
    ICypressNodeProxyPtr DoGetProxy(
        TListNode* trunkNode,
        NTransactionServer::TTransaction* transaction) override;

    void DoDestroy(TListNode* node) override;

    void DoBranch(
        const TListNode* originatingNode,
        TListNode* branchedNode,
        const TLockRequest& lockRequest) override;
    void DoMerge(
        TListNode* originatingNode,
        TListNode* branchedNode) override;

    void DoClone(
        TListNode* sourceNode,
        TListNode* clonedTrunkNode,
        NYTree::IAttributeDictionary* inheritedAttributes,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override;

    bool HasBranchedChangesImpl(
        TListNode* originatingNode,
        TListNode* branchedNode) override;

    void DoSerializeNode(
        TListNode* node,
        TSerializeNodeContext* context) override;
    void DoMaterializeNode(
        TListNode* trunkNode,
        TMaterializeNodeContext* context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
