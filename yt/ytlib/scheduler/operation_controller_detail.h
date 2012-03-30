#pragma once

#include "private.h"
#include "operation_controller.h"

#include <ytlib/logging/tagged_logger.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/chunk_server/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): extract to a proper place
class TOperationControllerBase
    : public IOperationController
{
public:
    TOperationControllerBase(IOperationHost* host, TOperation* operation);

    virtual void Initialize();
    virtual TFuture<TVoid>::TPtr Prepare();
    virtual TFuture<TVoid>::TPtr Revive();

    virtual void OnJobRunning(TJobPtr job);
    virtual void OnJobCompleted(TJobPtr job);
    virtual void OnJobFailed(TJobPtr job);


    virtual void OnOperationAborted();

    virtual void ScheduleJobs(
        TExecNodePtr node,
        std::vector<TJobPtr>* jobsToStart,
        std::vector<TJobPtr>* jobsToAbort);

protected:
    IOperationHost* Host;
    TOperation* Operation;

    NCypress::TCypressServiceProxy CypressProxy;
    NLog::TTaggedLogger Logger;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(BackgroundThread);


    void OnOperationFailed(const TError& error);

    void OnOperationCompleted();

    TFuture<TVoid>::TPtr OnInitComplete(TValueOrError<TVoid> result);

    virtual void AbortOperation();
};

////////////////////////////////////////////////////////////////////

// TODO(babenko): extract to a proper place
class TChunkPool
{
public:
    TChunkPool(TOperationPtr operation);

    int PutChunk(const NTableClient::NProto::TInputChunk& chunk, i64 weight);

    const NTableClient::NProto::TInputChunk& GetChunk(int index);

    void AllocateChunks(
        const Stroka& address,
        i64 maxWeight,
        std::vector<int>* indexes,
        i64* allocatedWeight,
        int* localCount,
        int* remoteCount);

    void DeallocateChunks(const std::vector<int>& indexes);

private:
    NLog::TTaggedLogger Logger;

    struct TChunkInfo
    {
        NTableClient::NProto::TInputChunk Chunk;
        i64 Weight;
    };

    std::vector<TChunkInfo> ChunkInfos;
    yhash_map<Stroka, yhash_set<int> > AddressToIndexSet;
    yhash_set<int> UnallocatedIndexes;

    void RegisterChunk(int index);

    void UnregisterChunk(int index);
};

////////////////////////////////////////////////////////////////////

// TODO(babenko): extract to a proper place
class TChunkListPool
    : public TRefCounted
{
public:
    TChunkListPool(
        NRpc::IChannel::TPtr masterChannel,
        IInvoker::TPtr controlInvoker,
        TOperationPtr operation,
        const TTransactionId& transactionId);

    int GetSize() const;

    NChunkServer::TChunkListId Extract();

    void Allocate(int count);

private:
    NRpc::IChannel::TPtr MasterChannel;
    IInvoker::TPtr ControlInvoker;
    TOperationPtr Operation;
    TTransactionId TransactionId;

    NLog::TTaggedLogger Logger;
    bool RequestInProgress;
    std::vector<NChunkServer::TChunkListId> Ids;

    void OnChunkListsCreated(NCypress::TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp);
};

typedef TIntrusivePtr<TChunkListPool> TChunkListPoolPtr;

////////////////////////////////////////////////////////////////////

// TODO(babenko): extract to a proper place
class TRunningCounter
{
public:
    TRunningCounter();

    void Init(i64 total);

    i64 GetTotal() const;
    i64 GetRunning() const;
    i64 GetDone() const;
    i64 GetPending() const;
    i64 GetFailed() const;


    void Start(i64 count);
    void Completed(i64 count);
    void Failed(i64 count);

private:
    i64 Total_;
    i64 Running_;
    i64 Done_;
    i64 Pending_;
    i64 Failed_;
};

Stroka ToString(const TRunningCounter& counter);

////////////////////////////////////////////////////////////////////

// TODO(babenko): move to a proper place

template <class T>
class TAsyncPipeline
    : public TRefCounted
{
public:
    TAsyncPipeline(
        IInvoker::TPtr invoker,
        TIntrusivePtr< IFunc< TIntrusivePtr < TFuture< TValueOrError<T> > > > > head)
        : Invoker(invoker)
        , Lazy(head)
    { }

    TIntrusivePtr< TFuture< TValueOrError<T> > > Run()
    {
        return Lazy->Do();
    }

    typedef T T1;

    template <class T2>
    TIntrusivePtr< TAsyncPipeline<T2> > Add(TIntrusivePtr< IParamFunc<T1, T2> > func)
    {
        auto wrappedFunc = FromFunctor([=] (TValueOrError<T1> x) -> TIntrusivePtr< TFuture< TValueOrError<T2> > > {
            if (!x.IsOK()) {
                return MakeFuture(TValueOrError<T2>(TError(x)));
            }
            try {
                auto y = func->Do(x.Value());
                return MakeFuture(TValueOrError<T2>(y));
            } catch (const std::exception& ex) {
                return MakeFuture(TValueOrError<T2>(TError(ex.what())));
            }
        });

        if (Invoker) {
            wrappedFunc = wrappedFunc->AsyncVia(Invoker);
        }

        auto lazy = Lazy;
        auto newLazy = FromFunctor([=] () {
            return lazy->Do()->Apply(wrappedFunc);
        });

        return New< TAsyncPipeline<T2> >(Invoker, newLazy);
    }

    template <class T2>
    TIntrusivePtr< TAsyncPipeline<T2> > Add(TIntrusivePtr< IParamFunc<T1, TIntrusivePtr< TFuture<T2> > > > func)
    {
        auto toValueOrError = FromFunctor([] (T2 x) {
            return TValueOrError<T2>(x);
        });

        auto wrappedFunc = FromFunctor([=] (TValueOrError<T1> x) -> TIntrusivePtr< TFuture< TValueOrError<T2> > > {
            if (!x.IsOK()) {
                return MakeFuture(TValueOrError<T2>(TError(x)));
            }
            try {
                auto y = func->Do(x.Value());
                return y->Apply(toValueOrError);
            } catch (const std::exception& ex) {
                return MakeFuture(TValueOrError<T2>(TError(ex.what())));
            }
        });

        if (Invoker) {
            wrappedFunc = wrappedFunc->AsyncVia(Invoker);
        }

        auto lazy = Lazy;
        auto newLazy = FromFunctor([=] () {
            return lazy->Do()->Apply(wrappedFunc);
        });

        return New< TAsyncPipeline<T2> >(Invoker, newLazy);
    }


private:
    IInvoker::TPtr Invoker;
    TIntrusivePtr< IFunc< TIntrusivePtr < TFuture< TValueOrError<T> > > > > Lazy;

};

TIntrusivePtr< TAsyncPipeline<TVoid> > StartAsyncPipeline(IInvoker::TPtr invoker = NULL);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
