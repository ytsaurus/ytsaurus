#pragma once

#include "public.h"
#include "lock.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/cypress_server/cypress_manager.pb.h>

#include <yt/server/object_server/object.h>

#include <yt/server/security_server/acl.h>
#include <yt/server/security_server/cluster_resources.h>

#include <yt/server/transaction_server/public.h>

#include <yt/ytlib/cypress_client/public.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>
#include <yt/core/misc/variant.h>

#include <yt/core/compression/public.h>

#include <yt/core/erasure/public.h>

#include <queue>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TVersionedBuiltinAttribute
{
public:
    struct TNull
    {
        void Persist(NCellMaster::TPersistenceContext& context);
    };

    struct TTombstone
    {
        void Persist(NCellMaster::TPersistenceContext& context);
    };

    // NB: Don't reorder the types; tags are used for persistence.
    using TBoxedT = TVariant<TNull, TTombstone, T>;

    template <class TOwner>
    const T& Get(
        TVersionedBuiltinAttribute<T> TOwner::*member,
        const TOwner* node) const;

    void Set(T value);
    void Reset();
    void Remove();

    template <class TOwner>
    void Merge(
        TVersionedBuiltinAttribute<T> TOwner::*member,
        TOwner* originatingNode,
        const TOwner* branchedNode);

    void Persist(NCellMaster::TPersistenceContext& context);

private:
    TBoxedT BoxedValue_{TNull()};

};

#define DEFINE_CYPRESS_BUILTIN_VERSIONED_ATTRIBUTE(ownerType, attrType, name) \
    private: \
        ::NYT::NCypressServer::TVersionedBuiltinAttribute<attrType> name##_; \
        \
    public: \
        const attrType& Get##name() const \
        { \
            return name##_.Get(&ownerType::name##_, this); \
        } \
        \
        void Set##name(attrType value) \
        { \
            name##_.Set(std::move(value)); \
        } \
        \
        void Reset##name() \
        { \
            name##_.Reset(); \
        } \
        \
        void Remove##name() \
        { \
            name##_.Remove(); \
        } \
        \
        void Merge##name(ownerType* originatingNode, const ownerType* branchedNode) \
        { \
            name##_.Merge(&ownerType::name##_, originatingNode, branchedNode); \
        }

////////////////////////////////////////////////////////////////////////////////

struct TCypressNodeDynamicData
    : public NObjectServer::TObjectDynamicData
{
    int AccessStatisticsUpdateIndex = -1;
    TNullable<TCypressNodeExpirationMap::iterator> ExpirationIterator;
};

////////////////////////////////////////////////////////////////////////////////

//! Provides a common base for all versioned (aka Cypress) nodes.
class TCypressNodeBase
    : public NObjectServer::TObjectBase
    , public TRefTracked<TCypressNodeBase>
{
public:
    //! For external nodes, this is the tag of the cell were the node
    //! was delegated to. For non-external nodes, this is #NotReplicatedCellTag.
    DEFINE_BYVAL_RW_PROPERTY(NObjectClient::TCellTag, ExternalCellTag, NObjectClient::NotReplicatedCellTag);

    //! If |false| then resource accounting ignores this node completely.
    //! Used by upload transactions, live preview etc.
    DEFINE_BYVAL_RW_PROPERTY(bool, AccountingEnabled, true);

    //! Contains all nodes with parent pointing here.
    //! When a node dies parent pointers of its immediate descendants are reset.
    DEFINE_BYREF_RW_PROPERTY(THashSet<TCypressNodeBase*>, ImmediateDescendants);

    DEFINE_BYVAL_RW_PROPERTY(ELockMode, LockMode, ELockMode::None);

    DEFINE_BYVAL_RW_PROPERTY(TCypressNodeBase*, TrunkNode);

    DEFINE_BYVAL_RW_PROPERTY(NTransactionServer::TTransaction*, Transaction);

    DEFINE_BYVAL_RW_PROPERTY(TInstant, CreationTime);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, ModificationTime);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, AccessTime);

    DEFINE_BYVAL_RW_PROPERTY(TNullable<TInstant>, ExpirationTime);

    DEFINE_BYVAL_RW_PROPERTY(i64, AccessCounter);

    DEFINE_BYVAL_RW_PROPERTY(i64, Revision);

    DEFINE_BYVAL_RW_PROPERTY(NSecurityServer::TAccount*, Account);
    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TClusterResources, CachedResourceUsage);
    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccessControlDescriptor, Acd);

    DEFINE_BYVAL_RW_PROPERTY(bool, Opaque);

    explicit TCypressNodeBase(const TVersionedNodeId& id);
    virtual ~TCypressNodeBase();

    TCypressNodeDynamicData* GetDynamicData() const;

    int GetAccessStatisticsUpdateIndex() const;
    void SetAccessStatisticsUpdateIndex(int value);

    TNullable<TCypressNodeExpirationMap::iterator> GetExpirationIterator() const;
    void SetExpirationIterator(TNullable<TCypressNodeExpirationMap::iterator> value);

    //! Returns the static type of the node.
    /*!
     *  \see NYT::NYTree::INode::GetType
     */
    virtual NYTree::ENodeType GetNodeType() const = 0;

    TCypressNodeBase* GetParent() const;
    void SetParent(TCypressNodeBase* parent);
    void ResetParent();

    TCypressNodeBase* GetOriginator() const;
    void SetOriginator(TCypressNodeBase* originator);

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

    // Similar methods are also declared in TObjectBase but starting from TCypressNodeBase
    // they become virtual.
    virtual void Save(NCellMaster::TSaveContext& context) const;
    virtual void Load(NCellMaster::TLoadContext& context);

private:
    TCypressNodeBase* Parent_ = nullptr;
    TCypressNodeBase* Originator_ = nullptr;
    std::unique_ptr<TCypressNodeLockingState> LockingState_;
    NTransactionServer::TTransactionId TransactionId_;

};

////////////////////////////////////////////////////////////////////////////////

struct TCypressNodeRefComparer
{
    static bool Compare(const TCypressNodeBase* lhs, const TCypressNodeBase* rhs);
};

NObjectServer::TVersionedObjectId GetObjectId(const TCypressNodeBase* object);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT

#define NODE_INL_H_
#include "node-inl.h"
#undef NODE_INL_H_
