#pragma once

#include "master_state_manager.h"

#include "../rpc/service.h"
#include "../rpc/server.h"

#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TMetaStateServiceBase
    : public NRpc::TServiceBase
{
protected:
    TMetaStateServiceBase(
        IInvoker::TPtr serviceInvoker,
        Stroka serviceName,
        Stroka loggingCategory)
        : NRpc::TServiceBase(
            serviceInvoker,
            serviceName,
            loggingCategory)
    { }

    void OnCommitError(NRpc::TServiceContext::TPtr context)
    {
        context->Reply(NRpc::EErrorCode::ServiceError);
    }
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
    const TMessage& message)
{
    TFixedChangeHeader fixedHeader;
    fixedHeader.HeaderSize = header.ByteSize();
    fixedHeader.MessageSize = message.ByteSize();

    TBlob data(sizeof (TFixedChangeHeader) + fixedHeader.HeaderSize + fixedHeader.MessageSize);
    
    Copy(
        reinterpret_cast<char*>(&fixedHeader),
        sizeof (TFixedChangeHeader),
        data.begin());
    YVERIFY(header.SerializeToArray(
        data.begin() + sizeof (TFixedChangeHeader),
        fixedHeader.HeaderSize));
    YVERIFY(message.SerializeToArray(
        data.begin() + sizeof (TFixedChangeHeader) + fixedHeader.HeaderSize,
        fixedHeader.MessageSize));
    return data;
}

inline void DeserializeChangeHeader(
    TRef changeData,
    NRpcMasterStateManager::TMsgChangeHeader* header)
{
    TFixedChangeHeader* fixedHeader = reinterpret_cast<TFixedChangeHeader*>(changeData.Begin());
    YVERIFY(header->ParseFromArray(
        changeData.Begin() + sizeof (fixedHeader),
        fixedHeader->HeaderSize));
}

inline void DeserializeChange(
    TRef changeData,
    NRpcMasterStateManager::TMsgChangeHeader* header,
    TRef* messageData)
{
    TFixedChangeHeader* fixedHeader = reinterpret_cast<TFixedChangeHeader*>(changeData.Begin());
    YVERIFY(header->ParseFromArray(
        changeData.Begin() + sizeof (TFixedChangeHeader),
        fixedHeader->HeaderSize));
    *messageData = TRef(
        changeData.Begin() + sizeof (TFixedChangeHeader) + fixedHeader->HeaderSize,
        fixedHeader->MessageSize);
}

////////////////////////////////////////////////////////////////////////////////

class TCompositeMetaState;

class TMetaStatePart
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TMetaStatePart> TPtr;

    template<class TMessage, class TResult>
    typename TAsyncResult<TResult>::TPtr ApplyChange(
        const TMessage& message,
        TIntrusivePtr< IParamFunc<const TMessage&, TResult> > changeMethod,
        IAction::TPtr errorHandler = NULL)
    {
        typename TUpdate<TMessage, TResult>::TPtr update = new TUpdate<TMessage, TResult>(
            StateManager,
            GetPartName(),
            message,
            changeMethod,
            errorHandler);
        return update->Run();
    }

protected:
    TMetaStatePart(TMasterStateManager::TPtr stateManager)
        : StateManager(stateManager)
    { }

    template<class TMessage, class TResult>
    void RegisterMethod(TIntrusivePtr< IParamFunc<const TMessage&, TResult> > changeMethod)
    {
        Stroka changeType = TMessage().GetTypeName();
        IParamAction<const TRef&>::TPtr action = FromMethod(
            &TMetaStatePart::MethodThunk<TMessage, TResult>,
            this,
            changeMethod);
        YVERIFY(Methods.insert(MakePair(changeType, action)).Second() == 1);
    }

    bool IsLeader() const
    {
        TMasterStateManager::EState state = StateManager->GetState();
        return state == TMasterStateManager::EState::Leading ||
               state == TMasterStateManager::EState::LeaderRecovery;
    }

    bool IsFolllower() const
    {
        TMasterStateManager::EState state = StateManager->GetState();
        return state == TMasterStateManager::EState::Following ||
               state == TMasterStateManager::EState::FollowerRecovery;
    }

    virtual Stroka GetPartName() const = 0;
    virtual TAsyncResult<TVoid>::TPtr Save(TOutputStream& output) = 0;
    virtual TAsyncResult<TVoid>::TPtr Load(TInputStream& input) = 0;
    virtual void Clear() = 0;

    TMasterStateManager::TPtr StateManager;
    IInvoker::TPtr SnapshotInvoker;

private:
    friend class TCompositeMetaState;

    void OnRegistered(IInvoker::TPtr snapshotInvoker)
    {
        SnapshotInvoker = snapshotInvoker;
    }

    void ApplyChange(Stroka changeType, TRef changeData)
    {
        TMethodMap::iterator it = Methods.find(changeType);
        YASSERT(it != Methods.end());
        it->Second()->Do(changeData);
    }

    template<class TMessage, class TResult>
    void MethodThunk(
        const TRef& changeData,
        typename IParamFunc<const TMessage&, TResult>::TPtr changeMethod)
    {
        google::protobuf::io::ArrayInputStream ais(changeData.Begin(), changeData.Size());
        TMessage message;
        YVERIFY(message.ParseFromZeroCopyStream(&ais));

        changeMethod->Do(message);
    }

    template<class TMessage, class TResult>
    class TUpdate
        : public TRefCountedBase
    {
    public:
        typedef TIntrusivePtr<TUpdate> TPtr;

        TUpdate(
            TMasterStateManager::TPtr stateManager,
            Stroka partName,
            const TMessage& message,
            typename IParamFunc<const TMessage&, TResult>::TPtr changeMethod,
            IAction::TPtr errorHandler)
            : StateManager(stateManager)
            , PartName(partName)
            , Message(message)
            , ChangeMethod(changeMethod)
            , ErrorHandler(errorHandler)
            , AsyncResult(new TAsyncResult<TResult>())
        { }

        typename TAsyncResult<TResult>::TPtr Run()
        {
            // TODO: change ns
            NRpcMasterStateManager::TMsgChangeHeader header;
            header.SetPartName(PartName);
            header.SetChangeType(Message.GetTypeName());

            TBlob changeData = SerializeChange(header, Message);

            StateManager
                ->CommitChange(
                    FromMethod(&TUpdate::InvokeChangeMethod, TPtr(this)),
                    TSharedRef(changeData))
                ->Subscribe(
                    FromMethod(&TUpdate::OnCommitted, TPtr(this)));

            return AsyncResult;
        }

    private:
        void InvokeChangeMethod()
        {
            Result = ChangeMethod->Do(Message);
        }

        void OnCommitted(TMasterStateManager::ECommitResult commitResult)
        {
            if (commitResult == TMasterStateManager::ECommitResult::Committed) {
                AsyncResult->Set(Result);
            } else if (~ErrorHandler != NULL) {
                ErrorHandler->Do();
            }
        }

        TMasterStateManager::TPtr StateManager;
        Stroka PartName;
        TMessage Message;
        Stroka MethodName;
        typename IParamFunc<const TMessage&, TResult>::TPtr ChangeMethod;
        IAction::TPtr ErrorHandler;
        typename TAsyncResult<TResult>::TPtr AsyncResult;
        TResult Result;

    };


    typedef yhash_map<Stroka, IParamAction<const TRef&>::TPtr> TMethodMap;
    TMethodMap Methods;

};

////////////////////////////////////////////////////////////////////////////////

class TCompositeMetaState
    : public IMasterState 
{
public:
    typedef TIntrusivePtr<TCompositeMetaState> TPtr;

    TCompositeMetaState()
        : StateInvoker(new TActionQueue())
        , SnapshotInvoker(new TActionQueue())
    { }

    void RegisterPart(TMetaStatePart::TPtr part)
    {
        Stroka partName = part->GetPartName();
        YVERIFY(Parts.insert(MakePair(partName, part)).Second());
        part->OnRegistered(SnapshotInvoker);
    }

    virtual IInvoker::TPtr GetInvoker() const
    {
        return StateInvoker;
    }

private:
    IInvoker::TPtr StateInvoker;
    IInvoker::TPtr SnapshotInvoker;

    typedef yhash_map<Stroka, TMetaStatePart::TPtr> TPartMap;
    TPartMap Parts;

    virtual TAsyncResult<TVoid>::TPtr Save(TOutputStream& output)
    {
        TAsyncResult<TVoid>::TPtr result;
        for (TPartMap::iterator it = Parts.begin();
             it != Parts.end();
             ++it)
        {
            result = it->Second()->Save(output);
        }
        return result;
    }

    virtual TAsyncResult<TVoid>::TPtr Load(TInputStream& input)
    {
        TAsyncResult<TVoid>::TPtr result;
        for (TPartMap::iterator it = Parts.begin();
             it != Parts.end();
             ++it)
        {
            result = it->Second()->Load(input);
        }
        return result;
    }

    virtual void ApplyChange(TRef changeData)
    {
        NRpcMasterStateManager::TMsgChangeHeader header;
        TRef messageData;
        DeserializeChange(
            changeData,
            &header,
            &messageData);

        Stroka partName = header.GetPartName();
        Stroka changeType = header.GetChangeType();

        TPartMap::iterator it = Parts.find(partName);
        YASSERT(it != Parts.end());

        TMetaStatePart::TPtr part = it->Second();
        part->ApplyChange(changeType, messageData);
    }

    virtual void Clear()
    {
        for (TPartMap::iterator it = Parts.begin();
             it != Parts.end();
             ++it)
        {
            it->Second()->Clear();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

#define METASTATE_REGISTER_METHOD(method) \
    RegisterMethod(FromMethod(&TThis::method, this))

#define METASTATE_APPLY_RPC_CHANGE(method, handler) \
    State \
        ->ApplyChange( \
            message, \
            FromMethod(&TState::method, State), \
            FromMethod(&TThis::OnCommitError, this, context->GetUntypedContext())) \
        ->Subscribe( \
            FromMethod(&TThis::handler, this, context))

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
