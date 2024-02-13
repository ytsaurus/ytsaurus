#pragma once

#include "public.h"
#include "lock.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/cypress_server/proto/cypress_manager.pb.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/security_server/acl.h>
#include <yt/yt/server/master/security_server/cluster_resources.h>
#include <yt/yt/server/master/security_server/detailed_master_memory.h>

#include <yt/yt/server/master/tablet_server/tablet_resources.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/ytlib/cypress_client/public.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/library/erasure/public.h>

#include <library/cpp/yt/memory/ref_tracked.h>

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
struct TVersionedBuiltinAttributeTraits
{
    using TRawType = T;
    static constexpr bool IsPointer = std::is_pointer_v<T>;
    static T ToRaw(T value);
    static T FromRaw(T value);
};

template <class T>
struct TVersionedBuiltinAttributeTraits<NObjectServer::TStrongObjectPtr<T>>
{
    using TRawType = T*;
    static constexpr bool IsPointer = true;
    static T* ToRaw(const NObjectServer::TStrongObjectPtr<T>& value);
    static NObjectServer::TStrongObjectPtr<T> FromRaw(T* value);
};

template <class T>
using TRawVersionedBuiltinAttributeType = typename TVersionedBuiltinAttributeTraits<T>::TRawType;

template <class T>
class TVersionedBuiltinAttribute
{
    static_assert(std::copyable<T> || NObjectServer::CClonable<T>,
        "TVersionedBuiltinAttribute requires T to be either copyable or clonable");

public:
    void Persist(const NCellMaster::TPersistenceContext& context);

    void Save(TBeginCopyContext& context) const;
    void Load(TEndCopyContext& context);

public:
    TVersionedBuiltinAttribute() noexcept = default;
    TVersionedBuiltinAttribute(const TVersionedBuiltinAttribute&) = delete;
    TVersionedBuiltinAttribute& operator=(const TVersionedBuiltinAttribute&) = delete;
    TVersionedBuiltinAttribute(TVersionedBuiltinAttribute&&) noexcept = default;
    TVersionedBuiltinAttribute& operator=(TVersionedBuiltinAttribute&&) noexcept = default;
    TVersionedBuiltinAttribute Clone() const;

    using TValue = T;

    std::optional<TRawVersionedBuiltinAttributeType<T>> ToOptional() const;

    //! Precondition: IsSet().
    TRawVersionedBuiltinAttributeType<T> Unbox() const;

    bool IsSet() const;
    bool IsNull() const;
    bool IsTombstoned() const;

    //! Sets this attribute to a new value.
    void Set(T value);
    //! Resets this attribute (to null).
    void Reset();
    //! Marks this attribute as removed (with a tombstone).
    void Remove();

    template <class TOwner>
    static TRawVersionedBuiltinAttributeType<T> Get(
        TVersionedBuiltinAttribute TOwner::*member,
        const TOwner* node);

    template <class TOwner>
    static TRawVersionedBuiltinAttributeType<T> Get(
        const TVersionedBuiltinAttribute<T>* (TOwner::*memberGetter)() const,
        const TOwner* node);

    template <class TOwner>
    static std::optional<TRawVersionedBuiltinAttributeType<T>> TryGet(
        TVersionedBuiltinAttribute<T> TOwner::*member,
        const TOwner* node);

    template <class TOwner>
    static std::optional<TRawVersionedBuiltinAttributeType<T>> TryGet(
        const TVersionedBuiltinAttribute<T>* (TOwner::* memberGetter)() const,
        const TOwner* node);

    void Merge(const TVersionedBuiltinAttribute& from, bool isTrunk);

private:
    std::conditional_t<
        TVersionedBuiltinAttributeTraits<T>::IsPointer,
        std::optional<T>, // std::nullopt is null, nullptr is tombstone.
        // NB: Don't reorder the types; tags are used for persistence.
        std::variant<TNullVersionedBuiltinAttribute, TTombstonedVersionedBuiltinAttribute, T>
    > BoxedValue_;
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
    } \
    static_assert(true)

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
    //! was delegated to. For non-external nodes, this is #NotReplicatedCellTagSentinel.
    DEFINE_BYVAL_RW_PROPERTY(NObjectClient::TCellTag, ExternalCellTag, NObjectClient::NotReplicatedCellTagSentinel);

    //! Contains all nodes with parent pointing here.
    //! When a node dies parent pointers of its immediate descendants are reset.
    DEFINE_BYREF_RW_PROPERTY(THashSet<TCypressNode*>, ImmediateDescendants);

    DEFINE_BYVAL_RW_PROPERTY(ELockMode, LockMode, ELockMode::None);

    DEFINE_BYVAL_RW_PROPERTY(TCypressNode*, TrunkNode);

    DEFINE_BYVAL_RW_PROPERTY(NTransactionServer::TTransaction*, Transaction);

    DEFINE_BYVAL_RW_PROPERTY(TInstant, CreationTime);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, ModificationTime);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, AccessTime);

    DEFINE_CYPRESS_BUILTIN_VERSIONED_ATTRIBUTE(TCypressNode, TInstant, ExpirationTime);

    //! Master memory usage account was already charged for.
    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TDetailedMasterMemory, ChargedDetailedMasterMemoryUsage);

    DEFINE_BYVAL_RW_PROPERTY(i64, AccessCounter);

    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccountPtr, Account);
    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccessControlDescriptor, Acd);

    DEFINE_BYVAL_RW_PROPERTY(bool, Opaque);
    DEFINE_CYPRESS_BUILTIN_VERSIONED_ATTRIBUTE(TCypressNode, TString, Annotation);

    //! The shard this node belongs to.
    //! Always null for foreign and non-trunk nodes.
    DEFINE_BYVAL_RW_PROPERTY(TCypressShard*, Shard);

    //! The corresponding node in Resolve Cache, if any.
    //! Always null for non-trunk nodes.
    DEFINE_BYVAL_RW_PROPERTY(TResolveCacheNodePtr, ResolveCacheNode);

    DEFINE_CYPRESS_BUILTIN_VERSIONED_ATTRIBUTE(TCypressNode, TDuration, ExpirationTimeout);

    struct TImmutableSequoiaProperties
    {
        const TString Key;
        const NYPath::TYPath Path;

        TImmutableSequoiaProperties(NYPath::TYPath key, TString path);

        bool operator==(const TImmutableSequoiaProperties& rhs) const noexcept = default;
        // Save/Load methods don't work with const fields, sadly.
    };
    DEFINE_BYREF_RW_PROPERTY(std::unique_ptr<TImmutableSequoiaProperties>, ImmutableSequoiaProperties);

    struct TMutableSequoiaProperties
    {
        bool BeingCreated = false;

        void Save(NCellMaster::TSaveContext& context) const;
        void Load(NCellMaster::TLoadContext& context);
    };
    DEFINE_BYREF_RW_PROPERTY(std::unique_ptr<TMutableSequoiaProperties>, MutableSequoiaProperties);

    using TObject::TObject;
    explicit TCypressNode(TVersionedNodeId id);
    virtual ~TCypressNode();

    // COMPAT(shakurov): delete the #branchIsOk parameters.
    TInstant GetTouchTime(bool branchIsOk = false) const;
    void SetTouchTime(TInstant touchTime, bool branchIsOk = false);

    void SetModified(NObjectServer::EModificationType modificationType) override;

    NHydra::TRevision GetNativeContentRevision() const;
    void SetNativeContentRevision(NHydra::TRevision);

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
    virtual NSecurityServer::TDetailedMasterMemory GetDetailedMasterMemoryUsage() const;

    //! Returns tablet resources of the node. Makes sense only for trunk nodes.
    virtual NTabletServer::TTabletResources GetTabletResourceUsage() const;

    TCypressNode* GetEffectiveExpirationTimeNode();
    TCypressNode* GetEffectiveExpirationTimeoutNode();

    //! Returns |true| if object is being created.
    bool IsBeingCreated() const;

    //! Returns true if the node can be cached for resolve.
    /*!
     *  The following conditions must be met:
     *  - node has no shared or exclusive locks
     *  - node is either either map-like, a link, or a portal entrance
     */
    bool CanCacheResolve() const;

    void CheckInvariants(NCellMaster::TBootstrap* bootstrap) const override;

    // Similar methods are also declared in TObject but starting from TCypressNode
    // they become virtual.
    virtual void Save(NCellMaster::TSaveContext& context) const;
    virtual void Load(NCellMaster::TLoadContext& context);

    void SaveEctoplasm(TStreamSaveContext& context) const override;
    void LoadEctoplasm(TStreamLoadContext& context) override;

    void VerifySequoia() const;

private:
    TCypressNode* Parent_ = nullptr;
    TCypressNode* Originator_ = nullptr;
    std::unique_ptr<TCypressNodeLockingState> LockingState_;
    NTransactionServer::TTransactionId TransactionId_;
    NHydra::TRevision NativeContentRevision_ = NHydra::NullRevision;

    // Only tracked for trunk nodes with non-null expiration timeout.
    TInstant TouchTime_;
};

DEFINE_MASTER_OBJECT_TYPE(TCypressNode)

////////////////////////////////////////////////////////////////////////////////

struct TCypressNodeIdComparer
{
    bool operator()(const TCypressNode* lhs, const TCypressNode* rhs) const;

    static bool Compare(const TCypressNode* lhs, const TCypressNode* rhs);
};

NObjectServer::TVersionedObjectId GetObjectId(const TCypressNode* object);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

#define NODE_INL_H_
#include "node-inl.h"
#undef NODE_INL_H_
