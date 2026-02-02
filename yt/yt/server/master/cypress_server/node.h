#pragma once

#include "public.h"
#include "lock.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/security_server/acl.h>
#include <yt/yt/server/master/security_server/cluster_resources.h>
#include <yt/yt/server/master/security_server/detailed_master_memory.h>

#include <yt/yt/server/master/tablet_server/public.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/server/lib/misc/assert_sizeof.h>

#include <yt/yt/ytlib/cypress_client/public.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/library/erasure/public.h>

#include <library/cpp/yt/memory/ref_tracked.h>

#include <queue>
#include <variant>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TClonableBuiltinAttributePtr
{
    static_assert(std::copyable<T> || NObjectServer::CClonable<T>);

public:
    TClonableBuiltinAttributePtr() noexcept;
    explicit TClonableBuiltinAttributePtr(const T& value);
    explicit TClonableBuiltinAttributePtr(T&& value);

    template <class... TArgs>
    TClonableBuiltinAttributePtr(std::in_place_t, TArgs&&... args);

    TClonableBuiltinAttributePtr(TClonableBuiltinAttributePtr&& other) noexcept = default;
    TClonableBuiltinAttributePtr& operator=(TClonableBuiltinAttributePtr&& other) noexcept = default;

    TClonableBuiltinAttributePtr Clone() const;

    explicit operator bool() const noexcept;

    T* operator->() const noexcept;

    T* Get() const noexcept;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

private:
    static std::unique_ptr<T> ConstructCopy(const T& value);

    std::unique_ptr<T> Value_;
};

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
    static constexpr bool IsInherentlyNullable = std::is_pointer_v<T>;
    static constexpr bool SerializeAsRaw = std::is_pointer_v<T>;
    static T ToRaw(T value);
    static T FromRaw(T value);
};

template <class T>
struct TVersionedBuiltinAttributeTraits<NObjectServer::TStrongObjectPtr<T>>
{
    using TRawType = T*;
    static constexpr bool IsInherentlyNullable = true;
    static constexpr bool SerializeAsRaw = true;
    static T* ToRaw(const NObjectServer::TStrongObjectPtr<T>& value);
    static NObjectServer::TStrongObjectPtr<T> FromRaw(T* value);
};

template <class T>
struct TVersionedBuiltinAttributeTraits<TClonableBuiltinAttributePtr<T>>
{
    using TRawType = T*;
    static constexpr bool IsInherentlyNullable = true;
    static constexpr bool SerializeAsRaw = false;
    static T* ToRaw(const TClonableBuiltinAttributePtr<T>& value);
    static TClonableBuiltinAttributePtr<T> FromRaw(T* value);
};

template <class T>
using TRawVersionedBuiltinAttributeType = typename TVersionedBuiltinAttributeTraits<T>::TRawType;

template <class T>
class TVersionedBuiltinAttribute
{
    static_assert(std::movable<T>);
    static_assert(std::copyable<T> || NObjectServer::CClonable<T>,
        "TVersionedBuiltinAttribute requires T to be either copyable or clonable");
    static_assert(
        !TVersionedBuiltinAttributeTraits<T>::SerializeAsRaw || TVersionedBuiltinAttributeTraits<T>::IsInherentlyNullable,
        "Serialization as raw is implemented only for inherently nullable types");

public:
    void Persist(const NCellMaster::TPersistenceContext& context);

    void Save(TSerializeNodeContext& context) const;
    void Load(TMaterializeNodeContext& context);

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
        TVersionedBuiltinAttributeTraits<T>::IsInherentlyNullable,
        std::optional<T>, // std::nullopt is null, value-initialized value is tombstone.
        // NB: Don't reorder the types; tags are used for persistence.
        std::variant<TNullVersionedBuiltinAttribute, TTombstonedVersionedBuiltinAttribute, T>
    > BoxedValue_;
};

#define DEFINE_CYPRESS_BUILTIN_VERSIONED_ATTRIBUTE(ownerType, attrType, name) \
private: \
    ::NYT::NCypressServer::TVersionedBuiltinAttribute<attrType> name##_; \
    \
public: \
    std::same_as<::NYT::NCypressServer::TRawVersionedBuiltinAttributeType<attrType>> auto Get##name() const \
    { \
        return ::NYT::NCypressServer::TVersionedBuiltinAttribute<attrType>::Get(&ownerType::name##_, this); \
    } \
    std::same_as<std::optional<::NYT::NCypressServer::TRawVersionedBuiltinAttributeType<attrType>>> auto TryGet##name() const \
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

// Think twice before increasing this.
YT_STATIC_ASSERT_SIZEOF_SANITY(TCypressNodeDynamicData, 48);

////////////////////////////////////////////////////////////////////////////////

//! Provides a common base for all versioned (aka Cypress) nodes.
class TCypressNode
    : public NObjectServer::TObject
    , public TRefTracked<TCypressNode>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TCypressNode*, TrunkNode);

    DEFINE_BYVAL_RW_PROPERTY(NTransactionServer::TTransaction*, Transaction);

    DEFINE_BYVAL_RW_PROPERTY(TInstant, CreationTime);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, ModificationTime);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, AccessTime);

    //! Master memory usage account was already charged for.
    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TDetailedMasterMemory, ChargedDetailedMasterMemoryUsage);

    DEFINE_BYVAL_RW_PROPERTY(i64, AccessCounter);

    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccountPtr, Account);
    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccessControlDescriptor, Acd);

    DEFINE_BYVAL_RW_PROPERTY(ELockMode, LockMode, ELockMode::None);

    DEFINE_BYVAL_RW_PROPERTY(bool, Opaque);
    DEFINE_BYVAL_RW_PROPERTY(bool, Reachable);

    //! For external nodes, this is the tag of the cell the node
    //! was delegated to. For non-external nodes, this is #NotReplicatedCellTagSentinel.
    DEFINE_BYVAL_RW_PROPERTY(NObjectClient::TCellTag, ExternalCellTag, NObjectClient::NotReplicatedCellTagSentinel);

    DEFINE_CYPRESS_BUILTIN_VERSIONED_ATTRIBUTE(TCypressNode, TString, Annotation);

    //! The shard this node belongs to.
    //! Always null for foreign and non-trunk nodes.
    DEFINE_BYVAL_RW_PROPERTY(TCypressShardRawPtr, Shard);

    //! The corresponding node in Resolve Cache, if any.
    //! Always null for non-trunk nodes.
    DEFINE_BYVAL_RW_PROPERTY(TResolveCacheNodePtr, ResolveCacheNode);

    template <std::copyable TTime>
    class TExpirationProperties
    {
    public:
        using TView = std::pair<NSecurityServer::TUserRawPtr, TTime>;

        TExpirationProperties() = default;
        TExpirationProperties(NSecurityServer::TUserPtr user, TTime time);
        explicit TExpirationProperties(TInstant lastReset);

        TExpirationProperties Clone() const;

        void Save(NCellMaster::TSaveContext& context) const;
        void Load(NCellMaster::TLoadContext& context);

        bool IsReset() const;

        std::optional<TView> AsView() const;
        std::optional<TInstant> GetLastResetTime() const;
        std::optional<NSecurityServer::TUserRawPtr> GetUser() const;
        std::optional<TTime> GetExpiration() const;

    private:
        using TLastResetInstant = TInstant;
        using TValue = std::pair<NSecurityServer::TUserPtr, TTime>;
        // NB: Don't reorder the types; tags are used for persistence.
        std::variant<TValue, TLastResetInstant> ExpirationValue_;
    };

    using TExpirationTimeProperties = TExpirationProperties<TInstant>;
    using TExpirationTimePropertiesPtr = TClonableBuiltinAttributePtr<TExpirationTimeProperties>;

    DEFINE_CYPRESS_BUILTIN_VERSIONED_ATTRIBUTE(TCypressNode, TExpirationTimePropertiesPtr, ExpirationTimeProperties);

    using TExpirationTimeoutProperties = TExpirationProperties<TDuration>;
    using TExpirationTimeoutPropertiesPtr = TClonableBuiltinAttributePtr<TExpirationTimeoutProperties>;

    DEFINE_CYPRESS_BUILTIN_VERSIONED_ATTRIBUTE(TCypressNode, TExpirationTimeoutPropertiesPtr, ExpirationTimeoutProperties);

    //! Used for both Sequoia nodes and Cypress link nodes. Note that link nodes
    //! do not have |ParentId| set up properly.
    struct TImmutableSequoiaProperties
    {
        const std::string Key;
        const NYPath::TYPath Path;
        // NB: Shouldn't be used for non-Sequoia (e.g. Cypress link) nodes.
        const TNodeId ParentId;

        TImmutableSequoiaProperties(std::string key, NYPath::TYPath path, TNodeId parentId);

        bool operator==(const TImmutableSequoiaProperties& rhs) const noexcept = default;
        // Save/Load methods don't work with const fields, sadly.
    };
    DEFINE_BYREF_RW_PROPERTY(std::unique_ptr<TImmutableSequoiaProperties>, ImmutableSequoiaProperties);

    struct TMutableSequoiaProperties
    {
        // TODO(kvk1920): get rid of this. It's useless. We can just look at ID
        // to determine if it was generated by current tx since Sequoia object
        // ID contains start timestamp of Sequoia tx by which it was generated.
        // Sequoia tx even has a special method for it!
        bool BeingCreated = false;

        // Removal of Cypress node in transaction is handled in parent map-node,
        // but we cannot do the same for Sequoia nodes. Therefore, we mark
        // removed branches as tombstone, merge this flag properly and remove
        // trunk node on topmost transaction commit.
        bool Tombstone = false;

        void Save(NCellMaster::TSaveContext& context) const;
        void Load(NCellMaster::TLoadContext& context);
    };
    DEFINE_BYREF_RW_PROPERTY(std::unique_ptr<TMutableSequoiaProperties>, MutableSequoiaProperties);

    using TObject::TObject;
    explicit TCypressNode(TVersionedNodeId id);

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

    std::optional<TExpirationTimeProperties::TView> GetExpirationTimePropertiesView() const;
    std::optional<TInstant> GetExpirationTime() const;
    std::optional<NSecurityServer::TUserRawPtr> GetExpirationTimeUser() const;
    std::optional<TInstant> GetExpirationTimeLastResetTime() const;

    std::optional<TExpirationTimeoutProperties::TView> GetExpirationTimeoutPropertiesView() const;
    std::optional<TDuration> GetExpirationTimeout() const;
    std::optional<NSecurityServer::TUserRawPtr> GetExpirationTimeoutUser() const;
    std::optional<TInstant> GetExpirationTimeoutLastResetTime() const;

    //! Returns the static type of the node.
    /*!
     *  \see NYT::NYTree::INode::GetType
     */
    virtual NYTree::ENodeType GetNodeType() const = 0;

    TCompositeCypressNode* GetParent() const;
    void SetParent(TCompositeCypressNode* parent);

    //! In contrast to |SetParent(nullptr)|, does not update the set of immediate descendats,
    //! just zeroes out the parent pointer.
    void DropParent();

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
    TCompositeCypressNodeRawPtr Parent_;
    TCypressNodeRawPtr Originator_;
    std::unique_ptr<TCypressNodeLockingState> LockingState_;
    NTransactionServer::TTransactionId TransactionId_;
    NHydra::TRevision NativeContentRevision_ = NHydra::NullRevision;

    // Only tracked for trunk nodes with non-null expiration timeout.
    TInstant TouchTime_;
};

DEFINE_MASTER_OBJECT_TYPE(TCypressNode)

// Think twice before increasing this.
YT_STATIC_ASSERT_SIZEOF_SANITY(TCypressNode, 376);

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
