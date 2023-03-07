#pragma once

#include "cypress_manager.h"
#include "helpers.h"
#include "node.h"
#include "private.h"

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/serialize.h>

#include <yt/server/master/chunk_server/chunk_requisition.h>

#include <yt/server/master/object_server/attribute_set.h>
#include <yt/server/master/object_server/object_detail.h>
#include <yt/server/master/object_server/object_part_cow_ptr.h>
#include <yt/server/master/object_server/type_handler_detail.h>

#include <yt/server/master/security_server/account.h>
#include <yt/server/master/security_server/security_manager.h>

#include <yt/server/master/tablet_server/tablet_cell_bundle.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/core/misc/serialize.h>

#include <yt/core/ytree/ephemeral_node_factory.h>
#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/node_detail.h>
#include <yt/core/ytree/overlaid_attribute_dictionaries.h>
#include <yt/core/ytree/tree_builder.h>
#include <yt/core/ytree/proto/ypath.pb.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TNontemplateCypressNodeTypeHandlerBase
    : public INodeTypeHandler
{
public:
    explicit TNontemplateCypressNodeTypeHandlerBase(NCellMaster::TBootstrap* bootstrap);

    virtual NObjectServer::ETypeFlags GetFlags() const override;

    virtual void FillAttributes(
        TCypressNode* trunkNode,
        NYTree::IAttributeDictionary* inheritedAttributes,
        NYTree::IAttributeDictionary* explicitAttributes) override;

    virtual bool IsSupportedInheritableAttribute(const TString& /*key*/) const;

protected:
    NCellMaster::TBootstrap* const Bootstrap_;

    NObjectServer::TObjectTypeMetadata Metadata_;


    bool IsLeader() const;
    bool IsRecovery() const;
    const TDynamicCypressManagerConfigPtr& GetDynamicCypressManagerConfig() const;

    void DestroyCore(TCypressNode* node);

    bool BeginCopyCore(
        TCypressNode* node,
        TBeginCopyContext* context);
    TCypressNode* EndCopyCore(
        TEndCopyContext* context,
        ICypressNodeFactory* factory,
        TNodeId sourceNodeId,
        bool* needCustomEndCopy);
    void EndCopyInplaceCore(
        TCypressNode* trunkNode,
        TEndCopyContext* context,
        ICypressNodeFactory* factory,
        TNodeId sourceNodeId);
    bool LoadInplace(
        TCypressNode* trunkNode,
        TEndCopyContext* context,
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
        TCypressNode* originatingNode);

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
};

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TCypressNodeTypeHandlerBase
    : public TNontemplateCypressNodeTypeHandlerBase
{
public:
    explicit TCypressNodeTypeHandlerBase(NCellMaster::TBootstrap* bootstrap)
        : TNontemplateCypressNodeTypeHandlerBase(bootstrap)
    { }

    virtual ICypressNodeProxyPtr GetProxy(
        TCypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction) override
    {
        return DoGetProxy(trunkNode->As<TImpl>(), transaction);
    }

    virtual i64 GetStaticMasterMemoryUsage() const override
    {
        return sizeof(TImpl);
    }

    virtual std::unique_ptr<TCypressNode> Instantiate(
        const TVersionedNodeId& id,
        NObjectClient::TCellTag externalCellTag) override
    {
        std::unique_ptr<TCypressNode> nodeHolder(new TImpl(id));
        nodeHolder->SetExternalCellTag(externalCellTag);
        nodeHolder->SetTrunkNode(nodeHolder.get());

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (nodeHolder->GetNativeCellTag() != multicellManager->GetCellTag()) {
            nodeHolder->SetForeign();
        }

        return nodeHolder;
    }

    virtual std::unique_ptr<TCypressNode> Create(
        TNodeId hintId,
        const TCreateNodeContext& context) override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(GetObjectType(), hintId);
        return DoCreate(TVersionedNodeId(id), context);
    }

    virtual void Destroy(TCypressNode* node) override
    {
        // Run core stuff.
        DestroyCore(node);

        // Run custom stuff.
        DoDestroy(node->As<TImpl>());
    }

    virtual void BeginCopy(
        TCypressNode* node,
        TBeginCopyContext* context) override
    {
        if (BeginCopyCore(node, context)) {
            DoBeginCopy(node->As<TImpl>(), context);
        }
    }

    virtual TCypressNode* EndCopy(
        TEndCopyContext* context,
        ICypressNodeFactory* factory,
        TNodeId sourceNodeId) override
    {
        bool needCustomEndCopy;
        auto* trunkNode = EndCopyCore(context, factory, sourceNodeId, &needCustomEndCopy);

        if (needCustomEndCopy) {
            DoEndCopy(trunkNode->template As<TImpl>(), context, factory);
        }

        return trunkNode;
    }

    virtual void EndCopyInplace(
        TCypressNode* trunkNode,
        TEndCopyContext* context,
        ICypressNodeFactory* factory,
        TNodeId sourceNodeId) override
    {
        EndCopyInplaceCore(trunkNode, context, factory, sourceNodeId);
        DoEndCopy(trunkNode->template As<TImpl>(), context, factory);
    }

    virtual std::unique_ptr<TCypressNode> Branch(
        TCypressNode* originatingNode,
        NTransactionServer::TTransaction* transaction,
        const TLockRequest& lockRequest) override
    {
        // Instantiate a branched copy.
        auto originatingId = originatingNode->GetVersionedId();
        auto branchedId = TVersionedNodeId(originatingId.ObjectId, GetObjectId(transaction));
        auto branchedNodeHolder = std::make_unique<TImpl>(branchedId);
        auto* typedBranchedNode = branchedNodeHolder.get();

        // Run core stuff.
        auto* typedOriginatingNode = originatingNode->As<TImpl>();
        BranchCorePrologue(typedOriginatingNode, typedBranchedNode, transaction, lockRequest);

        // Run custom stuff.
        DoBranch(typedOriginatingNode, typedBranchedNode, lockRequest);
        DoLogBranch(typedOriginatingNode, typedBranchedNode, lockRequest);

        BranchCoreEpilogue(typedBranchedNode);
        return std::move(branchedNodeHolder);
    }

    virtual void Unbranch(
        TCypressNode* originatingNode,
        TCypressNode* branchedNode) override
    {
        // Run custom stuff.
        auto* typedOriginatingNode = originatingNode->As<TImpl>();
        auto* typedBranchedNode = branchedNode->As<TImpl>();
        DoUnbranch(typedOriginatingNode, typedBranchedNode);
        DoLogUnbranch(typedOriginatingNode, typedBranchedNode);
    }

    virtual void Merge(
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

        // Update only originating node, because ResetAccount is called for branched node.
        MergeCoreEpilogue(typedOriginatingNode);
    }

    virtual TCypressNode* Clone(
        TCypressNode* sourceNode,
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
        DoClone(typedSourceNode, typedClonedTrunkNode, factory, mode, account);

        // Run core epilogue stuff.
        CloneCoreEpilogue(
            sourceNode,
            clonedTrunkNode,
            factory,
            mode);

        return clonedTrunkNode;
    }

    virtual bool HasBranchedChanges(
        TCypressNode* originatingNode,
        TCypressNode* branchedNode) override
    {
        return HasBranchedChangesImpl(
            originatingNode->template As<TImpl>(),
            branchedNode->template As<TImpl>());
    }

protected:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TImpl* trunkNode,
        NTransactionServer::TTransaction* transaction) = 0;

    virtual std::unique_ptr<TImpl> DoCreate(
        const NCypressServer::TVersionedNodeId& id,
        const TCreateNodeContext& context)
    {
        auto nodeHolder = std::make_unique<TImpl>(id);
        nodeHolder->SetExternalCellTag(context.ExternalCellTag);
        nodeHolder->SetTrunkNode(nodeHolder.get());

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (nodeHolder->GetNativeCellTag() != multicellManager->GetCellTag()) {
            nodeHolder->SetForeign();
        }

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* user = securityManager->GetAuthenticatedUser();
        securityManager->ValidatePermission(context.Account, user, NSecurityServer::EPermission::Use);
        // Null is passed as transaction because DoCreate() always creates trunk nodes.
        securityManager->SetAccount(
            nodeHolder.get(),
            context.Account,
            nullptr /* transaction */);

        return nodeHolder;
    }

    virtual void DoDestroy(TImpl* node)
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ResetAccount(node);
    }

    virtual void DoBeginCopy(
        TImpl* /*node*/,
        TBeginCopyContext* /*context*/)
    { }

    virtual void DoEndCopy(
        TImpl* /*trunkNode*/,
        TEndCopyContext* /*context*/,
        ICypressNodeFactory* /*factory*/)
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
        YT_LOG_DEBUG_UNLESS(
            IsRecovery(),
            "Node branched (OriginatingNodeId: %v, BranchedNodeId: %v, Mode: %v, LockTimestamp: %llx)",
            originatingNode->GetVersionedId(),
            branchedNode->GetVersionedId(),
            lockRequest.Mode,
            lockRequest.Timestamp);
    }

    virtual void DoMerge(
        TImpl* originatingNode,
        TImpl* branchedNode)
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ResetAccount(branchedNode);

        auto oldExpirationTime = originatingNode->TryGetExpirationTime();
        originatingNode->MergeExpirationTime(originatingNode, branchedNode);
        auto newExpirationTime = originatingNode->TryGetExpirationTime();
        if (originatingNode->IsTrunk() && newExpirationTime != oldExpirationTime) {
            const auto& cypressManager = Bootstrap_->GetCypressManager();
            cypressManager->SetExpirationTime(originatingNode, newExpirationTime);
        }
    }

    virtual void DoLogMerge(
        TImpl* originatingNode,
        TImpl* branchedNode)
    {
        const auto& Logger = CypressServerLogger;
        YT_LOG_DEBUG_UNLESS(
            IsRecovery(),
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
};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class TValue>
struct TCypressScalarTypeTraits
{ };

template <>
struct TCypressScalarTypeTraits<TString>
    : public NYTree::NDetail::TScalarTypeTraits<TString>
{
    static const NObjectClient::EObjectType ObjectType;
    static const NYTree::ENodeType NodeType;
};

template <>
struct TCypressScalarTypeTraits<i64>
    : public NYTree::NDetail::TScalarTypeTraits<i64>
{
    static const NObjectClient::EObjectType ObjectType;
    static const NYTree::ENodeType NodeType;
};

template <>
struct TCypressScalarTypeTraits<ui64>
    : public NYTree::NDetail::TScalarTypeTraits<ui64>
{
    static const NObjectClient::EObjectType ObjectType;
    static const NYTree::ENodeType NodeType;
};

template <>
struct TCypressScalarTypeTraits<double>
    : public NYTree::NDetail::TScalarTypeTraits<double>
{
    static const NObjectClient::EObjectType ObjectType;
    static const NYTree::ENodeType NodeType;
};

template <>
struct TCypressScalarTypeTraits<bool>
    : public NYTree::NDetail::TScalarTypeTraits<bool>
{
    static const NObjectClient::EObjectType ObjectType;
    static const NYTree::ENodeType NodeType;
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TScalarNode
    : public TCypressNode
{
public:
    DEFINE_BYREF_RW_PROPERTY(TValue, Value)

public:
    explicit TScalarNode(const TVersionedNodeId& id)
        : TCypressNode(id)
        , Value_()
    { }

    virtual NYTree::ENodeType GetNodeType() const override
    {
        return NDetail::TCypressScalarTypeTraits<TValue>::NodeType;
    }

    virtual void Save(NCellMaster::TSaveContext& context) const override
    {
        TCypressNode::Save(context);

        using NYT::Save;
        Save(context, Value_);
    }

    virtual void Load(NCellMaster::TLoadContext& context) override
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

    virtual NObjectClient::EObjectType GetObjectType() const override
    {
        return NDetail::TCypressScalarTypeTraits<TValue>::ObjectType;
    }

    virtual NYTree::ENodeType GetNodeType() const override
    {
        return NDetail::TCypressScalarTypeTraits<TValue>::NodeType;
    }

protected:
    typedef TCypressNodeTypeHandlerBase<TScalarNode<TValue>> TBase;

    virtual ICypressNodeProxyPtr DoGetProxy(
        TScalarNode<TValue>* trunkNode,
        NTransactionServer::TTransaction* transaction) override;

    virtual void DoBranch(
        const TScalarNode<TValue>* originatingNode,
        TScalarNode<TValue>* branchedNode,
        const TLockRequest& lockRequest) override
    {
        TBase::DoBranch(originatingNode, branchedNode, lockRequest);

        branchedNode->Value() = originatingNode->Value();
    }

    virtual void DoMerge(
        TScalarNode<TValue>* originatingNode,
        TScalarNode<TValue>* branchedNode) override
    {
        TBase::DoMerge(originatingNode, branchedNode);

        originatingNode->Value() = branchedNode->Value();
    }

    virtual void DoClone(
        TScalarNode<TValue>* sourceNode,
        TScalarNode<TValue>* clonedTrunkNode,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override
    {
        TBase::DoClone(sourceNode, clonedTrunkNode, factory, mode, account);

        clonedTrunkNode->Value() = sourceNode->Value();
    }

    virtual void DoBeginCopy(
        TScalarNode<TValue>* node,
        TBeginCopyContext* context) override
    {
        TBase::DoBeginCopy(node, context);

        using NYT::Save;
        Save(*context, node->Value());
    }

    virtual void DoEndCopy(
        TScalarNode<TValue>* trunkNode,
        TEndCopyContext* context,
        ICypressNodeFactory* factory) override
    {
        TBase::DoEndCopy(trunkNode, context, factory);

        using NYT::Load;
        Load(*context, trunkNode->Value());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCompositeNodeBase
    : public TCypressNode
{
public:
    using TCypressNode::TCypressNode;

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

    bool HasInheritableAttributes() const;

    // NB: the list of inheritable attributes doesn't include the "account"
    // attribute because that's already present on every Cypress node.

    std::optional<NCompression::ECodec> GetCompressionCodec() const;
    void SetCompressionCodec(std::optional<NCompression::ECodec> compressionCodec);

    std::optional<NErasure::ECodec> GetErasureCodec() const;
    void SetErasureCodec(std::optional<NErasure::ECodec> erasureCodec);

    std::optional<int> GetPrimaryMediumIndex() const;
    void SetPrimaryMediumIndex(std::optional<int> primaryMediumIndex);

    std::optional<NChunkServer::TChunkReplication> GetMedia() const;
    void SetMedia(std::optional<NChunkServer::TChunkReplication> media);

    // Although both Vital and ReplicationFactor can be deduced from Media, it's
    // important to be able to specify just the ReplicationFactor (or the Vital
    // flag) while leaving Media null.

    std::optional<int> GetReplicationFactor() const;
    void SetReplicationFactor(std::optional<int> replicationFactor);

    std::optional<bool> GetVital() const;
    void SetVital(std::optional<bool> vital);

    NTabletServer::TTabletCellBundle* GetTabletCellBundle() const;
    void SetTabletCellBundle(NTabletServer::TTabletCellBundle* cellBundle);

    std::optional<NTransactionClient::EAtomicity> GetAtomicity() const;
    void SetAtomicity(std::optional<NTransactionClient::EAtomicity> atomicity);

    std::optional<NTransactionClient::ECommitOrdering> GetCommitOrdering() const;
    void SetCommitOrdering(std::optional<NTransactionClient::ECommitOrdering> commitOrdering);

    std::optional<NTabletClient::EInMemoryMode> GetInMemoryMode() const;
    void SetInMemoryMode(std::optional<NTabletClient::EInMemoryMode> inMemoryMode);

    std::optional<NTableClient::EOptimizeFor> GetOptimizeFor() const;
    void SetOptimizeFor(std::optional<NTableClient::EOptimizeFor> optimizeFor);

    struct TAttributes
    {
        std::optional<NCompression::ECodec> CompressionCodec;
        std::optional<NErasure::ECodec> ErasureCodec;
        std::optional<int> PrimaryMediumIndex;
        std::optional<NChunkServer::TChunkReplication> Media;
        std::optional<int> ReplicationFactor;
        std::optional<bool> Vital;
        NTabletServer::TTabletCellBundle* TabletCellBundle = nullptr;
        std::optional<NTransactionClient::EAtomicity> Atomicity;
        std::optional<NTransactionClient::ECommitOrdering> CommitOrdering;
        std::optional<NTabletClient::EInMemoryMode> InMemoryMode;
        std::optional<NTableClient::EOptimizeFor> OptimizeFor;

        bool operator==(const TAttributes& rhs) const;
        bool operator!=(const TAttributes& rhs) const;

        void Persist(NCellMaster::TPersistenceContext& context);
        void Persist(NCypressServer::TCopyPersistenceContext& context);

        void Save(NCellMaster::TSaveContext& context) const;
        void Load(NCellMaster::TLoadContext& context);

        void Save(NCypressServer::TBeginCopyContext& context) const;
        void Load(NCypressServer::TEndCopyContext& context);

        // Are all attributes not null?
        bool AreFull() const;

        // Are all attributes null?
        bool AreEmpty() const;
    };

    const TAttributes* FindAttributes() const;
    void SetAttributes(const TAttributes* attributes);

private:
    std::unique_ptr<TAttributes> Attributes_;
};

// Beware: changing these macros changes snapshot format.
#define FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE(process) \
    process(CompressionCodec, compression_codec) \
    process(ErasureCodec, erasure_codec) \
    process(ReplicationFactor, replication_factor) \
    process(Vital, vital) \
    process(Atomicity, atomicity) \
    process(CommitOrdering, commit_ordering) \
    process(InMemoryMode, in_memory_mode) \
    process(OptimizeFor, optimize_for)

#define FOR_EACH_INHERITABLE_ATTRIBUTE(process) \
    FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE(process) \
    process(PrimaryMediumIndex, primary_medium) \
    process(Media, media) \
    process(TabletCellBundle, tablet_cell_bundle)

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
    virtual void DoDestroy(TImpl* node) override;

    virtual void DoClone(
        TImpl* sourceNode,
        TImpl* clonedTrunkNode,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override;

    virtual void DoBranch(
        const TImpl* originatingNode,
        TImpl* branchedNode,
        const TLockRequest& lockRequest) override;
    virtual void DoUnbranch(
        TImpl* originatingNode,
        TImpl* branchedNode) override;
    virtual void DoMerge(
        TImpl* originatingNode,
        TImpl* branchedNode) override;
    virtual bool HasBranchedChangesImpl(
        TImpl* originatingNode,
        TImpl* branchedNode) override;

    virtual void DoBeginCopy(
        TImpl* node,
        TBeginCopyContext* context) override;
    virtual void DoEndCopy(
        TImpl* trunkNode,
        TEndCopyContext* context,
        ICypressNodeFactory* factory) override;
};

////////////////////////////////////////////////////////////////////////////////

//! The core of a map node. May be shared between multiple map nodes for CoW optimization.
//! Designed to be wrapped into TObjectPartCoWPtr.
class TMapNodeChildren
{
public:
    using TKeyToChild = THashMap<TString, TCypressNode*>;
    using TChildToKey = THashMap<TCypressNode*, TString>;

    TMapNodeChildren() = default;
    ~TMapNodeChildren();

    // Refcounted classes never are - and shouldn't be - copyable.
    TMapNodeChildren(const TMapNodeChildren&) = delete;
    TMapNodeChildren& operator=(const TMapNodeChildren&) = delete;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void RecomputeMasterMemoryUsage();

    void Set(const NObjectServer::TObjectManagerPtr& objectManager, const TString& key, TCypressNode* child);
    void Insert(const NObjectServer::TObjectManagerPtr& objectManager, const TString& key, TCypressNode* child);
    void Remove(const NObjectServer::TObjectManagerPtr& objectManager, const TString& key, TCypressNode* child);
    bool Contains(const TString& key) const;

    const TKeyToChild& KeyToChild() const;
    const TChildToKey& ChildToKey() const;

    int GetRefCount() const noexcept;
    void Ref() noexcept;
    void Unref() noexcept;

    static void Destroy(TMapNodeChildren* children, const NObjectServer::TObjectManagerPtr& objectManager);
    static void Clear(TMapNodeChildren* children);
    static TMapNodeChildren* Copy(TMapNodeChildren* srcChildren, const NObjectServer::TObjectManagerPtr& objectManager);

    DEFINE_BYVAL_RO_PROPERTY(i64, MasterMemoryUsage);

private:
    void RefChildren(const NObjectServer::TObjectManagerPtr& objectManager);
    void UnrefChildren(const NObjectServer::TObjectManagerPtr& objectManager);

    TKeyToChild KeyToChild_;
    TChildToKey ChildToKey_;
    int RefCount_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TMapNode
    : public TCompositeNodeBase
{
public:
    using TKeyToChild = TMapNodeChildren::TKeyToChild;
    using TChildToKey = TMapNodeChildren::TChildToKey;

    DEFINE_BYREF_RW_PROPERTY(int, ChildCountDelta);

public:
    explicit TMapNode(const TVersionedNodeId& id);

    ~TMapNode();

    explicit TMapNode(const TMapNode&) = delete;
    TMapNode& operator=(const TMapNode&) = delete;

    const TKeyToChild& KeyToChild() const;
    const TChildToKey& ChildToKey() const;

    // Potentially does the 'copy' part of CoW.
    TMapNodeChildren& MutableChildren(const NObjectServer::TObjectManagerPtr& objectManager);

    virtual NYTree::ENodeType GetNodeType() const override;

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

    virtual int GetGCWeight() const override;
    virtual i64 GetMasterMemoryUsage() const override;

    void AssignChildren(
        const NObjectServer::TObjectPartCoWPtr<TMapNodeChildren>& children,
        const NObjectServer::TObjectManagerPtr& objectManager);

private:
    NObjectServer::TObjectPartCoWPtr<TMapNodeChildren> Children_;

    template <class TImpl>
    friend class TMapNodeTypeHandlerImpl;
};

////////////////////////////////////////////////////////////////////////////////

// NB: The implementation of this template class can be found in _cpp_file,
// together with all relevant explicit instantiations.
template <class TImpl = TMapNode>
class TMapNodeTypeHandlerImpl
    : public TCompositeNodeTypeHandler<TImpl>
{
public:
    using TBase = TCompositeNodeTypeHandler<TImpl>;

    using TBase::TBase;

    virtual NObjectClient::EObjectType GetObjectType() const override;
    virtual NYTree::ENodeType GetNodeType() const override;

protected:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TImpl* trunkNode,
        NTransactionServer::TTransaction* transaction) override;

    virtual void DoDestroy(TImpl* node) override;

    virtual void DoBranch(
        const TImpl* originatingNode,
        TImpl* branchedNode,
        const TLockRequest& lockRequest) override;

    virtual void DoMerge(
        TImpl* originatingNode,
        TImpl* branchedNode) override;

    virtual void DoClone(
        TImpl* sourceNode,
        TImpl* clonedTrunkNode,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override;

    virtual bool HasBranchedChangesImpl(
        TImpl* originatingNode,
        TImpl* branchedNode) override;

    virtual void DoBeginCopy(
        TImpl* node,
        TBeginCopyContext* context) override;
    virtual void DoEndCopy(
        TImpl* trunkNode,
        TEndCopyContext* context,
        ICypressNodeFactory* factory) override;
};

using TMapNodeTypeHandler = TMapNodeTypeHandlerImpl<TMapNode>;

////////////////////////////////////////////////////////////////////////////////

class TListNode
    : public TCompositeNodeBase
{
private:
    using TBase = TCompositeNodeBase;

public:
    using TIndexToChild = std::vector<TCypressNode*>;
    using TChildToIndex = THashMap<TCypressNode*, int>;

    DEFINE_BYREF_RW_PROPERTY(TIndexToChild, IndexToChild);
    DEFINE_BYREF_RW_PROPERTY(TChildToIndex, ChildToIndex);

public:
    using TBase::TBase;

    virtual NYTree::ENodeType GetNodeType() const override;

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

    virtual int GetGCWeight() const override;
};

////////////////////////////////////////////////////////////////////////////////

class TListNodeTypeHandler
    : public TCompositeNodeTypeHandler<TListNode>
{
private:
    using TBase = TCompositeNodeTypeHandler<TListNode>;

public:
    using TBase::TBase;

    virtual NObjectClient::EObjectType GetObjectType() const override;
    virtual NYTree::ENodeType GetNodeType() const override;

private:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TListNode* trunkNode,
        NTransactionServer::TTransaction* transaction) override;

    virtual void DoDestroy(TListNode* node) override;

    virtual void DoBranch(
        const TListNode* originatingNode,
        TListNode* branchedNode,
        const TLockRequest& lockRequest) override;
    virtual void DoMerge(
        TListNode* originatingNode,
        TListNode* branchedNode) override;

    virtual void DoClone(
        TListNode* sourceNode,
        TListNode* clonedTrunkNode,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override;

    virtual bool HasBranchedChangesImpl(
        TListNode* originatingNode,
        TListNode* branchedNode) override;

    virtual void DoBeginCopy(
        TListNode* node,
        TBeginCopyContext* context) override;
    virtual void DoEndCopy(
        TListNode* trunkNode,
        TEndCopyContext* context,
        ICypressNodeFactory* factory) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
