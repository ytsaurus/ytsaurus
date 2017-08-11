#pragma once

#include <mapreduce/yt/interface/batch_request.h>
#include <mapreduce/yt/interface/fwd.h>
#include <mapreduce/yt/interface/node.h>

#include <mapreduce/yt/http/requests.h>

#include <library/threading/future/future.h>

#include <util/generic/ptr.h>
#include <util/generic/deque.h>

#include <exception>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

struct IRetryPolicy;
struct TResponseInfo;
class TClient;
using TClientPtr = ::TIntrusivePtr<TClient>;

////////////////////////////////////////////////////////////////////////////////

class TBatchRequestImpl
    : public TThrRefBase
{
public:
    struct IResponseItemParser
        : public TThrRefBase
    {
        ~IResponseItemParser() = default;

        virtual void SetResponse(TMaybe<TNode> node) = 0;
        virtual void SetException(std::exception_ptr e) = 0;
    };

public:
    TBatchRequestImpl();
    ~TBatchRequestImpl();

    bool IsExecuted() const;
    void MarkExecuted();

    void FillParameterList(size_t maxSize, TNode* result, TInstant* nextTry) const;

    size_t BatchSize() const;

    void ParseResponse(
        const TResponseInfo& requestResult,
        const IRetryPolicy& retryPolicy,
        TBatchRequestImpl* retryBatch,
        TInstant now = TInstant::Now());
    void ParseResponse(
        TNode response,
        const TString& requestId,
        const IRetryPolicy& retryPolicy,
        TBatchRequestImpl* retryBatch,
        TInstant now = TInstant::Now());
    void SetErrorResult(std::exception_ptr e) const;

    NThreading::TFuture<TNodeId> Create(
        const TTransactionId& transaction,
        const TYPath& path,
        ENodeType type,
        const TCreateOptions& options);
    NThreading::TFuture<void> Remove(
        const TTransactionId& transaction,
        const TYPath& path,
        const TRemoveOptions& options);
    NThreading::TFuture<bool> Exists(
        const TTransactionId& transaction,
        const TYPath& path);
    NThreading::TFuture<TNode> Get(
        const TTransactionId& transaction,
        const TYPath& path,
        const TGetOptions& options);
    NThreading::TFuture<void> Set(
        const TTransactionId& transaction,
        const TYPath& path,
        const TNode& value);
    NThreading::TFuture<TNode::TList> List(
        const TTransactionId& transaction,
        const TYPath& path,
        const TListOptions& options);
    NThreading::TFuture<TNodeId> Copy(
        const TTransactionId& transaction,
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TCopyOptions& options);
    NThreading::TFuture<TNodeId> Move(
        const TTransactionId& transaction,
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TMoveOptions& options);
    NThreading::TFuture<TNodeId> Link(
        const TTransactionId& transaction,
        const TYPath& targetPath,
        const TYPath& linkPath,
        const TLinkOptions& options);
    NThreading::TFuture<TLockId> Lock(
        const TTransactionId& transaction,
        const TYPath& path,
        ELockMode mode, const TLockOptions& options);
    NThreading::TFuture<TRichYPath> CanonizeYPath(const TRichYPath& path);

private:
    struct TBatchItem {
        TNode Parameters;
        ::TIntrusivePtr<IResponseItemParser> ResponseParser;
        TInstant NextTry;

        TBatchItem(TNode parameters, ::TIntrusivePtr<IResponseItemParser> responseParser);

        TBatchItem(const TBatchItem& batchItem, TInstant nextTry);
    };

private:
    template <typename TResponseParser>
    typename TResponseParser::TFutureResult AddRequest(
        const TString& command,
        TNode parameters,
        TMaybe<TNode> input);

    template <typename TResponseParser>
    typename TResponseParser::TFutureResult AddRequest(
        const TString& command,
        TNode parameters,
        TMaybe<TNode> input,
        TIntrusivePtr<TResponseParser> parser);

    void AddRequest(TBatchItem batchItem);

private:
    ydeque<TBatchItem> BatchItemList_;
    bool Executed_ = false;
};

////////////////////////////////////////////////////////////////////

class TBatchRequest
    : public IBatchRequest
{
public:
    TBatchRequest(const TTransactionId& defaultTransaction, ::TIntrusivePtr<TClient> client);

    ~TBatchRequest();

    virtual IBatchRequestBase& WithTransaction(const TTransactionId& transactionId) override;

    virtual NThreading::TFuture<TLockId> Create(
        const TYPath& path,
        ENodeType type,
        const TCreateOptions& options = TCreateOptions()) override;

    virtual NThreading::TFuture<void> Remove(
        const TYPath& path,
        const TRemoveOptions& options = TRemoveOptions()) override;

    virtual NThreading::TFuture<bool> Exists(const TYPath& path) override;

    virtual NThreading::TFuture<TNode> Get(
        const TYPath& path,
        const TGetOptions& options = TGetOptions()) override;

    virtual NThreading::TFuture<void> Set(
        const TYPath& path,
        const TNode& node) override;

    virtual NThreading::TFuture<TNode::TList> List(
        const TYPath& path,
        const TListOptions& options = TListOptions()) override;

    virtual NThreading::TFuture<TNodeId> Copy(
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TCopyOptions& options = TCopyOptions()) override;

    virtual NThreading::TFuture<TNodeId> Move(
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TMoveOptions& options = TMoveOptions()) override;

    virtual NThreading::TFuture<TNodeId> Link(
        const TYPath& targetPath,
        const TYPath& linkPath,
        const TLinkOptions& options = TLinkOptions()) override;

    virtual NThreading::TFuture<ILockPtr> Lock(
        const TYPath& path,
        ELockMode mode,
        const TLockOptions& options = TLockOptions()) override;

    virtual NThreading::TFuture<TRichYPath> CanonizeYPath(const TRichYPath& path) override;

    virtual void ExecuteBatch(const TExecuteBatchOptions& executeBatch) override;

private:
    TBatchRequest(NDetail::TBatchRequestImpl* impl, ::TIntrusivePtr<TClient> client);

private:
    TTransactionId DefaultTransaction_;
    ::TIntrusivePtr<NDetail::TBatchRequestImpl> Impl_;
    THolder<TBatchRequest> TmpWithTransaction_;
    ::TIntrusivePtr<TClient> Client_;

private:
    friend class NYT::NDetail::TClient;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
