#include "batch_request_impl.h"
#include "rpc_parameters_serialization.h"

#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/interface/node.h>
#include <mapreduce/yt/http/error.h>
#include <mapreduce/yt/http/retry_request.h>

#include <util/generic/guid.h>

#include <exception>

namespace NYT {
namespace NDetail {

using NThreading::TFuture;
using NThreading::TPromise;
using NThreading::NewPromise;

////////////////////////////////////////////////////////////////////

static void EnsureNothing(const TMaybe<TNode>& node)
{
    if (node) {
        ythrow yexception()
            << "Internal error: expected to have no response got response of type: "
            << TNode::TypeToString(node->GetType());
    }
}

static void EnsureSomething(const TMaybe<TNode>& node)
{
    if (!node) {
        ythrow yexception()
            << "Internal error: expected to have response of any type got no response.";
    }
}

static void EnsureType(const TNode& node, TNode::EType type)
{
    if (node.GetType() != type) {
        ythrow yexception() << "Internal error: unexpected response type. "
            << "Expected: " << TNode::TypeToString(type)
            << " actual: " << TNode::TypeToString(node.GetType());
    }
}

static void EnsureType(const TMaybe<TNode>& node, TNode::EType type)
{
    if (!node) {
        ythrow yexception()
            << "Internal error: expected to have response of type "
            << TNode::TypeToString(type) << " got no response.";
    }

    EnsureType(*node, type);
}

////////////////////////////////////////////////////////////////////

template <typename TReturnType>
class TResponseParserBase
    : public TBatchRequestImpl::IResponseItemParser
{
public:
    using TFutureResult = TFuture<TReturnType>;

public:
    TResponseParserBase()
        : Result(NewPromise<TReturnType>())
    { }

    virtual void SetException(std::exception_ptr e) override
    {
        Result.SetException(std::move(e));
    }

    TFuture<TReturnType> GetFuture()
    {
        return Result.GetFuture();
    }

protected:
    TPromise<TReturnType> Result;
};

////////////////////////////////////////////////////////////////////


class TGetResponseParser
    : public TResponseParserBase<TNode>
{
public:
    virtual void SetResponse(TMaybe<TNode> node) override
    {
        EnsureSomething(node);
        Result.SetValue(std::move(*node));
    }
};

////////////////////////////////////////////////////////////////////

class TVoidResponseParser
    : public TResponseParserBase<void>
{
public:
    virtual void SetResponse(TMaybe<TNode> node) override
    {
        EnsureNothing(node);
        Result.SetValue();
    }
};

////////////////////////////////////////////////////////////////////

class TListResponseParser
    : public TResponseParserBase<TNode::TList>
{
public:
    virtual void SetResponse(TMaybe<TNode> node) override
    {
        EnsureType(node, TNode::LIST);
        Result.SetValue(std::move(node->AsList()));
    }
};

////////////////////////////////////////////////////////////////////

class TExistsResponseParser
    : public TResponseParserBase<bool>
{
public:
    virtual void SetResponse(TMaybe<TNode> node) override
    {
        EnsureType(node, TNode::BOOL);
        Result.SetValue(std::move(node->AsBool()));
    }
};

////////////////////////////////////////////////////////////////////

class TGuidResponseParser
    : public TResponseParserBase<TGUID>
{
public:
    virtual void SetResponse(TMaybe<TNode> node) override
    {
        EnsureType(node, TNode::STRING);
        Result.SetValue(GetGuid(node->AsString()));
    }
};

////////////////////////////////////////////////////////////////////

TBatchRequestImpl::TBatchItem::TBatchItem(TNode parameters, ::TIntrusivePtr<IResponseItemParser> responseParser)
    : Parameters(std::move(parameters))
    , ResponseParser(std::move(responseParser))
    , NextTry()
{ }

TBatchRequestImpl::TBatchItem::TBatchItem(const TBatchItem& batchItem, TInstant nextTry)
    : Parameters(batchItem.Parameters)
    , ResponseParser(batchItem.ResponseParser)
    , NextTry(nextTry)
{ }

////////////////////////////////////////////////////////////////////

TBatchRequestImpl::TBatchRequestImpl() = default;

TBatchRequestImpl::~TBatchRequestImpl() = default;

void TBatchRequestImpl::MarkExecuted()
{
    if (Executed_) {
        ythrow yexception() << "Cannot execute batch request since it is alredy executed";
    }
    Executed_ = true;
}

template <typename TResponseParser>
typename TResponseParser::TFutureResult TBatchRequestImpl::AddRequest(
    const Stroka& command,
    TNode parameters,
    TMaybe<TNode> input)
{
    if (Executed_) {
        ythrow yexception() << "Cannot add request: batch request is already executed";
    }
    TNode request;
    request["command"] = command;
    request["parameters"] = std::move(parameters);
    if (input) {
        request["input"] = std::move(*input);
    }
    auto parser = MakeIntrusive<TResponseParser>();
    BatchItemList_.emplace_back(std::move(request), parser);
    return parser->GetFuture();
}

void TBatchRequestImpl::AddRequest(TBatchItem batchItem)
{
    if (Executed_) {
        ythrow yexception() << "Cannot add request: batch request is already executed";
    }
    BatchItemList_.push_back(batchItem);
}

TFuture<TNodeId> TBatchRequestImpl::Create(
    const TTransactionId& transaction,
    const TYPath& path,
    ENodeType type,
    const TCreateOptions& options)
{
    return AddRequest<TGuidResponseParser>(
        "create",
        SerializeParamsForCreate(transaction, path, type, options),
        Nothing());
}

TFuture<void> TBatchRequestImpl::Remove(
    const TTransactionId& transaction,
    const TYPath& path,
    const TRemoveOptions& options)
{
    return AddRequest<TVoidResponseParser>(
        "remove",
        SerializeParamsForRemove(transaction, path, options),
        Nothing());
}

TFuture<bool> TBatchRequestImpl::Exists(const TTransactionId& transaction, const TYPath& path)
{
    return AddRequest<TExistsResponseParser>(
        "exists",
        SerializeParamsForExists(transaction, path),
        Nothing());
}

TFuture<TNode> TBatchRequestImpl::Get(
    const TTransactionId& transaction,
    const TYPath& path,
    const TGetOptions& options)
{
    return AddRequest<TGetResponseParser>(
        "get",
        SerializeParamsForGet(transaction, path, options),
        Nothing());
}

TFuture<void> TBatchRequestImpl::Set(
    const TTransactionId& transaction,
    const TYPath& path,
    const TNode& node)
{
    return AddRequest<TVoidResponseParser>(
        "set",
        SerializeParamsForSet(transaction, path),
        node);
}

TFuture<TNode::TList> TBatchRequestImpl::List(
    const TTransactionId& transaction,
    const TYPath& path,
    const TListOptions& options)
{
    return AddRequest<TListResponseParser>(
        "list",
        SerializeParamsForList(transaction, path, options),
        Nothing());
}

TFuture<TNodeId> TBatchRequestImpl::Copy(
    const TTransactionId& transaction,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TCopyOptions& options)
{
    return AddRequest<TGuidResponseParser>(
        "copy",
        SerializeParamsForCopy(transaction, sourcePath, destinationPath, options),
        Nothing());
}

TFuture<TNodeId> TBatchRequestImpl::Move(
    const TTransactionId& transaction,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TMoveOptions& options)
{
    return AddRequest<TGuidResponseParser>(
        "move",
        SerializeParamsForMove(transaction, sourcePath, destinationPath, options),
        Nothing());
}

TFuture<TNodeId> TBatchRequestImpl::Link(
    const TTransactionId& transaction,
    const TYPath& targetPath,
    const TYPath& linkPath,
    const TLinkOptions& options)
{
    return AddRequest<TGuidResponseParser>(
        "link",
        SerializeParamsForLink(transaction, targetPath, linkPath, options),
        Nothing());
}

TFuture<TLockId> TBatchRequestImpl::Lock(
    const TTransactionId& transaction,
    const TYPath& path,
    ELockMode mode,
    const TLockOptions& options)
{
    return AddRequest<TGuidResponseParser>(
        "lock",
        SerializeParamsForLock(transaction, path, mode, options),
        Nothing());
}

void TBatchRequestImpl::FillParameterList(size_t maxSize, TNode* result, TInstant* nextTry) const
{
    Y_VERIFY(result);
    Y_VERIFY(nextTry);

    *nextTry = TInstant();
    maxSize = Min(maxSize, BatchItemList_.size());
    *result = TNode::CreateList();
    for (size_t i = 0; i < maxSize; ++i) {
        result->Add(BatchItemList_[i].Parameters);
        if (BatchItemList_[i].NextTry > *nextTry) {
            *nextTry = BatchItemList_[i].NextTry;
        }
    }
}

void TBatchRequestImpl::ParseResponse(
    const TResponseInfo& requestResult,
    const IRetryPolicy& retryPolicy,
    TBatchRequestImpl* retryBatch,
    TInstant now)
{
    TNode node = NodeFromYsonString(requestResult.Response);
    return ParseResponse(node, requestResult.RequestId, retryPolicy, retryBatch, now);
}

void TBatchRequestImpl::ParseResponse(
    TNode node,
    const Stroka& requestId,
    const IRetryPolicy& retryPolicy,
    TBatchRequestImpl* retryBatch,
    TInstant now)
{
    Y_VERIFY(retryBatch);

    EnsureType(node, TNode::LIST);
    auto& responseList = node.AsList();
    const auto size = responseList.size();
    if (size > BatchItemList_.size()) {
        ythrow yexception() << "Size of server response exceeds size of batch request; "
            " size of batch: " << BatchItemList_.size() <<
            " size of server response: " << size << '.';
    }

    for (size_t i = 0; i != size; ++i) {
        try {
            EnsureType(responseList[i], TNode::MAP);
            auto& responseNode = responseList[i].AsMap();
            const auto outputIt = responseNode.find("output");
            if (outputIt != responseNode.end()) {
                BatchItemList_[i].ResponseParser->SetResponse(std::move(outputIt->second));
            } else {
                const auto errorIt = responseNode.find("error");
                if (errorIt == responseNode.end()) {
                    BatchItemList_[i].ResponseParser->SetResponse(Nothing());
                } else {
                    TErrorResponse error(400, requestId);
                    error.SetError(TError(errorIt->second));
                    if (auto curInterval = retryPolicy.GetRetryInterval(error)) {
                        retryBatch->AddRequest(TBatchItem(BatchItemList_[i], now + *curInterval));
                    } else {
                        BatchItemList_[i].ResponseParser->SetException(std::make_exception_ptr(error));
                    }
                }
            }
        } catch (const yexception& e) {
            // We don't expect other exceptions, so we don't catch (...)
            BatchItemList_[i].ResponseParser->SetException(std::current_exception());
        }
    }
    BatchItemList_.erase(BatchItemList_.begin(), BatchItemList_.begin() + size);
}

void TBatchRequestImpl::SetErrorResult(std::exception_ptr e) const
{
    for (const auto& batchItem : BatchItemList_) {
        batchItem.ResponseParser->SetException(e);
    }
}

size_t TBatchRequestImpl::BatchSize() const
{
    return BatchItemList_.size();
}

////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
