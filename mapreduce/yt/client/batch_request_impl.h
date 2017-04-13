#pragma once

#include <mapreduce/yt/interface/node.h>

#include <library/threading/future/future.h>

#include <util/generic/ptr.h>

#include <exception>

namespace NYT {
namespace NDetail {

struct IRetryPolicy;
struct TResponseInfo;

////////////////////////////////////////////////////////////////////////////////

class TBatchRequestImpl
    : public TThrRefBase
{
public:
    struct IResponseItemParser;

public:
    TBatchRequestImpl();
    ~TBatchRequestImpl();

    void MarkExecuted();

    const TNode& GetParameterList() const;

    size_t BatchSize() const;

    void ParseResponse(const TResponseInfo& requestResult, const IRetryPolicy& retryPolicy, TDuration* maxRetryInterval);
    void ParseResponse(TNode response, const Stroka& requestId, const IRetryPolicy& retryPolicy, TDuration* maxRetryInterval);
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

private:
    template <typename TResponseParser>
    typename TResponseParser::TFutureResult AddRequest(
        const Stroka& command,
        TNode parameters,
        TMaybe<TNode> input);

private:
    yvector<::TIntrusivePtr<IResponseItemParser>> ResponseParserList_;
    TNode RequestList_ = TNode::CreateList();
    bool Executed_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
