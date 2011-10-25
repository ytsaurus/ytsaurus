#pragma once

#include "common.h"
#include "node.h"
#include "lock.h"
#include "node_type.h"
#include "cypress_manager.pb.h"

#include "../transaction_manager/transaction.h"
#include "../transaction_manager/transaction_manager.h"
#include "../ytree/ypath.h"
#include "../misc/id_generator.h"
#include "../meta_state/meta_state_manager.h"
#include "../meta_state/composite_meta_state.h"
#include "../meta_state/map.h"
#include "../meta_state/meta_change.h"

namespace NYT {
namespace NCypress {

using NTransaction::TTransaction;
using NTransaction::NullTransactionId;
using NTransaction::TTransactionManager;
using NMetaState::TMetaChange;

////////////////////////////////////////////////////////////////////////////////

class TCypressManager
    : public NMetaState::TMetaStatePart
{
public:
    typedef TCypressManager TThis;
    typedef TIntrusivePtr<TThis> TPtr;

    TCypressManager(
        NMetaState::TMetaStateManager::TPtr metaStateManager,
        NMetaState::TCompositeMetaState::TPtr metaState,
        TTransactionManager::TPtr transactionManager);

    void RegisterDynamicType(IDynamicTypeHandler::TPtr handler);

    METAMAP_ACCESSORS_DECL(Node, ICypressNode, TBranchedNodeId);

    INode::TPtr FindNode(
        const TNodeId& nodeId,
        const TTransactionId& transactionId);

    INode::TPtr GetNode(
        const TNodeId& nodeId,
        const TTransactionId& transactionId);

    void RefNode(ICypressNode& node);
    void UnrefNode(ICypressNode & node);

    IStringNode::TPtr CreateStringNodeProxy(const TTransactionId& transactionId);
    IInt64Node::TPtr  CreateInt64NodeProxy(const TTransactionId& transactionId);
    IDoubleNode::TPtr CreateDoubleNodeProxy(const TTransactionId& transactionId);
    IMapNode::TPtr    CreateMapNodeProxy(const TTransactionId& transactionId);
    IListNode::TPtr   CreateListNodeProxy(const TTransactionId& transactionId);

    TYsonBuilder::TPtr GetYsonDeserializer(const TTransactionId& transactionId);
    INode::TPtr CreateDynamicNode(
        const TTransactionId& transactionId,
        IMapNode::TPtr description);
    TAutoPtr<ICypressNode> CreateDynamicNode(
        ERuntimeNodeType type,
        const TBranchedNodeId& id);

    METAMAP_ACCESSORS_DECL(Lock, TLock, TLockId);

    TLock& CreateLock(const TNodeId& nodeId, const TTransactionId& transactionId);

    ICypressNode& BranchNode(ICypressNode& node, const TTransactionId& transactionId);

    void GetYPath(
        const TTransactionId& transactionId,
        TYPath path,
        IYsonConsumer* consumer);

    TMetaChange<TVoid>::TPtr InitiateSetYPath(
        const TTransactionId& transactionId,
        TYPath path,
        const Stroka& value);

    TMetaChange<TVoid>::TPtr InitiateRemoveYPath(
        const TTransactionId& transactionId,
        TYPath path);

    TMetaChange<TVoid>::TPtr InitiateLockYPath(
        const TTransactionId& transactionId,
        TYPath path);

private:
    class TNodeMapTraits
    {
    public:
        TNodeMapTraits(TCypressManager::TPtr cypressManager)
            : CypressManager(cypressManager)
        { }

        TAutoPtr<ICypressNode> Clone(ICypressNode* value) const
        {
            return value->Clone();
        }

        void Save(ICypressNode* value, TOutputStream* output) const
        {
            ::Save(output, static_cast<i32>(value->GetRuntimeType()));
            ::Save(output, value->GetId());
            value->Save(output);
        }

        TAutoPtr<ICypressNode> Load(TInputStream* input) const
        {
            i32 intType;
            TBranchedNodeId id;
            ::Load(input, intType);
            ::Load(input, id);
            TAutoPtr<ICypressNode> value;
            auto type = ERuntimeNodeType(intType);
            switch (type) {
                case ERuntimeNodeType::String:
                    value = new TStringNode(id);
                    break;
                case ERuntimeNodeType::Int64:
                    value = new TInt64Node(id);
                    break;
                case ERuntimeNodeType::Double:
                    value = new TDoubleNode(id);
                    break;
                case ERuntimeNodeType::Map:
                    value = new TMapNode(id);
                    break;
                case ERuntimeNodeType::List:
                    value = new TListNode(id);
                    break;
                default:
                    value = CypressManager->CreateDynamicNode(type, id);
                    break;
            }
            value->Load(input);
            return value;
        }

    private:
        TCypressManager::TPtr CypressManager;
    };
    
    TTransactionManager::TPtr TransactionManager;

    TIdGenerator<TNodeId> NodeIdGenerator;
    NMetaState::TMetaStateMap<TBranchedNodeId, ICypressNode, TNodeMapTraits> NodeMap;

    TIdGenerator<TLockId> LockIdGenerator; 
    NMetaState::TMetaStateMap<TLockId, TLock> LockMap;

    yhash_map<ERuntimeNodeType, IDynamicTypeHandler::TPtr> RuntimeTypeToHandler;
    yhash_map<Stroka, IDynamicTypeHandler::TPtr> TypeNameToHandler;

    TVoid SetYPath(const NProto::TMsgSet& message);
    TVoid RemoveYPath(const NProto::TMsgRemove& message);
    TVoid LockYPath(const NProto::TMsgLock& message);

    // TMetaStatePart overrides.
    virtual Stroka GetPartName() const;
    virtual TFuture<TVoid>::TPtr Save(TOutputStream* stream, IInvoker::TPtr invoker);
    virtual TFuture<TVoid>::TPtr Load(TInputStream* stream, IInvoker::TPtr invoker);
    virtual void Clear();

    void CreateWorld();

    void OnTransactionCommitted(TTransaction& transaction);
    void OnTransactionAborted(TTransaction& transaction);

    void ReleaseLocks(TTransaction& transaction);
    void MergeBranchedNodes(TTransaction& transaction);
    void RemoveBranchedNodes(TTransaction& transaction);
    void CommitCreatedNodes(TTransaction& transaction);
    void RemoveCreatedNodes(TTransaction& transaction);

    template <class TImpl, class TProxy>
    TIntrusivePtr<TProxy> CreateNode(const TTransactionId& transactionId);

    class TYsonDeserializationConsumer;
    friend class TYsonDeserializationConsumer;
    INode::TPtr YsonDeserializerThunk(
        TYsonProducer::TPtr producer,
        const TTransactionId& transactionId);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
