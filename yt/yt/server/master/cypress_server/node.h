#pragma once

#include "public.h"
#include "lock.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/cypress_server/proto/cypress_manager.pb.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/security_server/acl.h>
#include <yt/yt/server/master/security_server/cluster_resources.h>

#include <yt/yt/server/master/tablet_server/tablet_resources.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/ytlib/cypress_client/public.h>

#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/ref_tracked.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/library/erasure/public.h>

#include <queue>
#include <variant>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

struct TNullVersionedBuiltinAttribute
{
    void Persist(const NCellMaster::TPersistenceContext& context);
    void Persist(const NCypressServer::TCopyPersistenceContext& context);
};

struct TTombstonedVersionedBuiltinAttribute
{
    void Persist(const NCellMaster::TPersistenceContext& context);
    void Persist(const NCypressServer::TCopyPersistenceContext& context);
};

template <class T>
class TVersionedBuiltinAttribute
{
public:
    void Persist(const NCellMaster::TPersistenceContext& context);
    void Persist(const NCypressServer::TCopyPersistenceContext& context);

public:
    using TValue = T;

    std::optional<T> ToOptional() const;

    //! Precondition: IsSet().
    T Unbox() const;

    bool IsSet() const;
    bool IsNull() const;
    bool IsTombstoned() const;

    //! Sets this attribute to a new value and returns the old value (if any).
    std::optional<T> Set(T value);
    //! Resets this attribute (to null) and returns the old value (if any).
    std::optional<T> Reset();
    //! Marks this attribute as removed (with a tombstone) and returns the old
    //! value (if any).
    std::optional<T> Remove();

    // COMPAT(shakurov): remove this.
    void SetOrReset(T value);

    template <class TOwner>
    static T Get(
        TVersionedBuiltinAttribute TOwner::*member,
        const TOwner* node);

    template <class TOwner>
    static T Get(
        const TVersionedBuiltinAttribute<T>* (TOwner::*memberGetter)() const,
        const TOwner* node);

    template <class TOwner>
    static std::optional<T> TryGet(
        TVersionedBuiltinAttribute<T> TOwner::*member,
        const TOwner* node);

    template <class TOwner>
    static std::optional<T> TryGet(
        const TVersionedBuiltinAttribute<T>* (TOwner::* memberGetter)() const,
        const TOwner* node);

    void Merge(const TVersionedBuiltinAttribute& from, bool isTrunk);

private:
    using TBox = std::conditional_t<
        std::is_pointer_v<T>,
        std::optional<T>, // std::nullopt is null, nullptr is tombstone.
        // NB: Don't reorder the types; tags are used for persistence.
        std::variant<TNullVersionedBuiltinAttribute, TTombstonedVersionedBuiltinAttribute, T>>;

    TBox BoxedValue_ = {};
};

#define DEFINE_CYPRESS_BUILTIN_VERSIONED_ATTRIBUTE(ownerType, attrType, name) \
private: \
    ::NYT::NCypressServer::TVersionedBuiltinAttribute<attrType> name##_; \
    \
public: \
    attrType Get##name() const \
    { \
        return ::NYT::NCypressServer::TVersionedBuiltinAttribute<attrType>::Get(&ownerType::name##_, this); \
    } \
    std::optional<attrType> TryGet##name() const \
    { \
        return ::NYT::NCypressServer::TVersionedBuiltinAttribute<attrType>::TryGet(&ownerType::name##_, this); \
    } \
    \
    void Set##name(attrType value) \
    { \
        name##_.Set(std::move(value)); \
    } \
    \
    void Remove##name() \
    { \
        if (IsTrunk()) { \
            name##_.Reset(); \
        } else { \
            name##_.Remove(); \
        } \
    } \
    \
    void Merge##name(const ownerType* branchedNode) \
    { \
        name##_.Merge(branchedNode->name##_, IsTrunk()); \
    }

////////////////////////////////////////////////////////////////////////////////

struct TCypressNodeDynamicData
    : public NObjectServer::TObjectDynamicData
{
    int AccessStatisticsUpdateIndex = -1;
    int TouchNodesIndex = -1;
    std::optional<TCypressNodeExpirationMap::iterator> ExpirationTimeIterator;
    std::optional<TCypressNodeExpirationMap::iterator> ExpirationTimeoutIterator;
};

////////////////////////////////////////////////////////////////////////////////

//! Provides a common base for all versioned (aka Cypress) nodes.
class TCypressNode
    : public NObjectServer::TObject
    , public TRefTracked<TCypressNode>
{
public:
    //! For external nodes, this is the tag of the cell were the node
    //! was delegated to. For non-external nodes, this is #NotReplicatedCellTag.
    DEFINE_BYVAL_RW_PROPERTY(NObjectClient::TCellTag, ExternalCellTag, NObjectClient::NotReplicatedCellTag);

    //! Contains all nodes with parent pointing here.
    //! When a node dies parent pointers of its immediate descendants are reset.
    DEFINE_BYREF_RW_PROPERTY(THashSet<TCypressNode*>, ImmediateDescendants);

    DEFINE_BYVAL_RW_PROPERTY(ELockMode, LockMode, ELockMode::None);

    DEFINE_BYVAL_RW_PROPERTY(TCypressNode*, TrunkNode);

    DEFINE_BYVAL_RW_PROPERTY(NTransactionServer::TTransaction*, Transaction);

    DEFINE_BYVAL_RW_PROPERTY(TInstant, CreationTime);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, ModificationTime);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, AccessTime);

    DEFINE_CYPRESS_BUILTIN_VERSIONED_ATTRIBUTE(TCypressNode, TInstant, ExpirationTime)
    DEFINE_CYPRESS_BUILTIN_VERSIONED_ATTRIBUTE(TCypressNode, TDuration, ExpirationTimeout)

    //! Master memory usage account was already charged for.
    DEFINE_BYVAL_RW_PROPERTY(i64, ChargedMasterMemoryUsage);
    DEFINE_BYVAL_RW_PROPERTY(i64, AccessCounter);

    DEFINE_BYVAL_RW_PROPERTY(NHydra::TRevision, AttributeRevision);
    DEFINE_BYVAL_RW_PROPERTY(NHydra::TRevision, ContentRevision);

    DEFINE_BYVAL_RW_PROPERTY(NSecurityServer::TAccount*, Account);
    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccessControlDescriptor, Acd);

    DEFINE_BYVAL_RW_PROPERTY(bool, Opaque);
    DEFINE_CYPRESS_BUILTIN_VERSIONED_ATTRIBUTE(TCypressNode, TString, Annotation)

    //! The shard this node belongs to.
    //! Always null for foreign and non-trunk nodes.
    DEFINE_BYVAL_RW_PROPERTY(TCypressShard*, Shard);

    //! The corresponding node in Resolve Cache, if any.
    //! Always null for non-trunk nodes.
    DEFINE_BYVAL_RW_PROPERTY(TResolveCacheNodePtr, ResolveCacheNode);

    explicit TCypressNode(const TVersionedNodeId& id);
    virtual ~TCypressNode();

    NHydra::TRevision GetRevision() const;

    TCypressNodeDynamicData* GetDynamicData() const;

    int GetAccessStatisticsUpdateIndex() const;
    void SetAccessStatisticsUpdateIndex(int value);

    int GetTouchNodesIndex() const;
    void SetTouchNodesIndex(int value);

    std::optional<TCypressNodeExpirationMap::iterator> GetExpirationTimeIterator() const;
    void SetExpirationTimeIterator(std::optional<TCypressNodeExpirationMap::iterator> value);

    std::optional<TCypressNodeExpirationMap::iterator> GetExpirationTimeoutIterator() const;
    void SetExpirationTimeoutIterator(std::optional<TCypressNodeExpirationMap::iterator> value);

    //! Returns the static type of the node.
    /*!
     *  \see NYT::NYTree::INode::GetType
     */
    virtual NYTree::ENodeType GetNodeType() const = 0;

    TCypressNode* GetParent() const;
    void SetParent(TCypressNode* parent);
    void ResetParent();

    TCypressNode* GetOriginator() const;
    void SetOriginator(TCypressNode* originator);

    const TCypressNodeLockingState& LockingState() const;
    TCypressNodeLockingState* MutableLockingState();
    bool HasLockingState() const;
    void ResetLockingState();
    void ResetLockingStateIfEmpty();

    //! Returns the composite (versioned) id of the node.
    TVersionedNodeId GetVersionedId() const;

    //! Returns |true| if the node is external, i.e. was delegated
    //! to another cell.
    bool IsExternal() const;

    //! Returns the resource usage of the node. For trunk nodes, this is total
    //! resource usage; for branched nodes, this is delta.
    virtual NSecurityServer::TClusterResources GetDeltaResourceUsage() const;

    //! Returns the resource usage as seen by the user.
    //! These values are exposed via |resource_usage| attribute.
    virtual NSecurityServer::TClusterResources GetTotalResourceUsage() const;

    //! Returns master memory occupied.
    //! This method is computationally cheap.
    virtual i64 GetMasterMemoryUsage() const;

    //! Returns tablet resources of the node. Makes sense only for trunk nodes.
    virtual NTabletServer::TTabletResources GetTabletResourceUsage() const;

    //! Returns |true| if object is being created.
    bool IsBeingCreated() const;

    //! Returns true if the node can be cached for resolve.
    /*!
     *  The following conditions must be met:
     *  - node has no shared or exclusive locks
     *  - node is either either map-like, a link, or a portal entrance
     */
    bool CanCacheResolve() const;

    // Similar methods are also declared in TObject but starting from TCypressNode
    // they become virtual.
    virtual void Save(NCellMaster::TSaveContext& context) const;
    virtual void Load(NCellMaster::TLoadContext& context);

private:
    TCypressNode* Parent_ = nullptr;
    TCypressNode* Originator_ = nullptr;
    std::unique_ptr<TCypressNodeLockingState> LockingState_;
    NTransactionServer::TTransactionId TransactionId_;

};

////////////////////////////////////////////////////////////////////////////////

struct TCypressNodeRefComparer
{
    static bool Compare(const TCypressNode* lhs, const TCypressNode* rhs);
};

NObjectServer::TVersionedObjectId GetObjectId(const TCypressNode* object);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

#define NODE_INL_H_
#include "node-inl.h"
#undef NODE_INL_H_
