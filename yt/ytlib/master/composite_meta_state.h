#pragma once

#include "master_state_manager.h"

#include "../rpc/server.h"

#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////


class TCompositeMetaState;

class TMetaStatePart
    : public virtual TRefCountedBase
{
public:
    typedef TIntrusivePtr<TMetaStatePart> TPtr;

    TMetaStatePart(
        TMasterStateManager::TPtr metaStateManager,
        TIntrusivePtr<TCompositeMetaState> metaState);

    template<class TMessage, class TResult>
    typename TAsyncResult<TResult>::TPtr CommitChange(
        const TMessage& message,
        TIntrusivePtr< IParamFunc<const TMessage&, TResult> > changeMethod,
        IAction::TPtr errorHandler = NULL);

protected:
    template<class TMessage, class TResult>
    void RegisterMethod(TIntrusivePtr< IParamFunc<const TMessage&, TResult> > changeMethod);

    template<class TThis, class TMessage, class TResult>
    void RegisterMethod(
        TThis* this_,
        TResult (TThis::* changeMethod)(const TMessage&))
    {
        RegisterMethod(FromMethod(changeMethod, this_));
    }

    bool IsLeader() const;
    bool IsFolllower() const;

    IInvoker::TPtr GetSnapshotInvoker() const;
    IInvoker::TPtr GetStateInvoker() const;
    IInvoker::TPtr GetEpochStateInvoker() const;

    virtual Stroka GetPartName() const = 0;

    virtual TAsyncResult<TVoid>::TPtr Save(TOutputStream& output) = 0;
    virtual TAsyncResult<TVoid>::TPtr Load(TInputStream& input) = 0;

    virtual void Clear() = 0;

    virtual void OnStartLeading();
    virtual void OnStopLeading();
    virtual void OnStartFollowing();
    virtual void OnStopFollowing();

    TMasterStateManager::TPtr MetaStateManager;
    TIntrusivePtr<TCompositeMetaState> MetaState;

private:
    friend class TCompositeMetaState;

    void CommitChange(Stroka changeType, TRef changeData);

    template<class TMessage, class TResult>
    void MethodThunk(
        const TRef& changeData,
        typename IParamFunc<const TMessage&, TResult>::TPtr changeMethod);

    template<class TMessage, class TResult>
    class TUpdate;

};

////////////////////////////////////////////////////////////////////////////////

class TCompositeMetaState
    : public IMasterState 
{
public:
    typedef TIntrusivePtr<TCompositeMetaState> TPtr;

    TCompositeMetaState();

    virtual IInvoker::TPtr GetInvoker() const;

    void RegisterPart(TMetaStatePart::TPtr part);

private:
    friend class TMetaStatePart;

    IInvoker::TPtr StateInvoker;
    IInvoker::TPtr SnapshotInvoker;
    TCancelableInvoker::TPtr EpochStateInvoker;

    typedef yhash_map<Stroka, IParamAction<const TRef&>::TPtr> TMethodMap;
    TMethodMap Methods;

    typedef yhash_map<Stroka, TMetaStatePart::TPtr> TPartMap;
    TPartMap Parts;

    virtual TAsyncResult<TVoid>::TPtr Save(TOutputStream& output);
    virtual TAsyncResult<TVoid>::TPtr Load(TInputStream& input);

    virtual void ApplyChange(const TRef& changeData);

    virtual void Clear();

    virtual void OnStartLeading();
    virtual void OnStopLeading();
    virtual void OnStartFollowing();
    virtual void OnStopFollowing();

    void StartEpoch();
    void StopEpoch();

};

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 4)

struct TFixedChangeHeader
{
    i32 HeaderSize;
    i32 MessageSize;
};

#pragma pack(pop)

template <class TMessage>
TBlob SerializeChange(
    const NRpcMasterStateManager::TMsgChangeHeader& header,
    const TMessage& message);

void DeserializeChangeHeader(
    TRef changeData,
    NRpcMasterStateManager::TMsgChangeHeader* header);

void DeserializeChange(
    TRef changeData,
    NRpcMasterStateManager::TMsgChangeHeader* header,
    TRef* messageData);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define COMPOSITE_META_STATE_INL_H_
#include "composite_meta_state-inl.h"
#undef COMPOSITE_META_STATE_INL_H_
