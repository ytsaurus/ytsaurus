#include "batch_request_impl.h"

#include "lock.h"
#include "rpc_parameters_serialization.h"

#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/finally_guard.h>
#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/common/log.h>
#include <mapreduce/yt/common/serialize.h>

#include <mapreduce/yt/interface/node.h>
#include <mapreduce/yt/http/error.h>
#include <mapreduce/yt/http/retry_request.h>

#include <util/generic/guid.h>
#include <util/string/builder.h>

#include <exception>

namespace NYT {
namespace NDetail {

using NThreading::TFuture;
using NThreading::TPromise;
using NThreading::NewPromise;
using TResponseContext = TBatchRequestImpl::TResponseContext;

////////////////////////////////////////////////////////////////////

static TString RequestInfo(const TNode& request)
{
    return TStringBuilder()
        << request["command"].AsString() << ' ' << NodeToYsonString(request["parameters"]);
}

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

struct TBatchRequestImpl::TResponseContext
{
    TClientPtr Client;
};

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
    virtual void SetResponse(TMaybe<TNode> node, const TResponseContext&) override
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
    virtual void SetResponse(TMaybe<TNode> node, const TResponseContext&) override
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
    virtual void SetResponse(TMaybe<TNode> node, const TResponseContext&) override
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
    virtual void SetResponse(TMaybe<TNode> node, const TResponseContext&) override
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
    virtual void SetResponse(TMaybe<TNode> node, const TResponseContext&) override
    {
        EnsureType(node, TNode::STRING);
        Result.SetValue(GetGuid(node->AsString()));
    }
};

////////////////////////////////////////////////////////////////////

class TLockResponseParser
    : public TResponseParserBase<ILockPtr>
{
public:
    explicit TLockResponseParser(bool waitable)
        : Waitable_(waitable)
    { }

    virtual void SetResponse(TMaybe<TNode> node, const TResponseContext& responseContext) override
    {
        EnsureType(node, TNode::STRING);

        auto lockId = GetGuid(node->AsString());
        if (Waitable_) {
            Result.SetValue(MakeIntrusive<TLock>(lockId, responseContext.Client));
        } else {
            Result.SetValue(MakeIntrusive<TLock>(lockId));
        }
    }
private:
    const bool Waitable_;
};

////////////////////////////////////////////////////////////////////

class TCanonizeYPathResponseParser
    : public TResponseParserBase<TRichYPath>
{
public:
    explicit TCanonizeYPathResponseParser(const TRichYPath& original)
        : OriginalNode_(PathToNode(original))
    { }

    virtual void SetResponse(TMaybe<TNode> node, const TResponseContext& /*responseContext*/) override
    {
        EnsureType(node, TNode::STRING);

        for (const auto& item : OriginalNode_.GetAttributes().AsMap()) {
            node->Attributes()[item.first] = item.second;
        }
        TRichYPath result;
        Deserialize(result, *node);
        result.Path_ = AddPathPrefix(result.Path_);
        Result.SetValue(result);
    }

private:
    TNode OriginalNode_;
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

bool TBatchRequestImpl::IsExecuted() const
{
    return Executed_;
}

void TBatchRequestImpl::MarkExecuted()
{
    Executed_ = true;
}

template <typename TResponseParser>
typename TResponseParser::TFutureResult TBatchRequestImpl::AddRequest(
    const TString& command,
    TNode parameters,
    TMaybe<TNode> input)
{
    return AddRequest(command, parameters, input, MakeIntrusive<TResponseParser>());
}

template <typename TResponseParser>
typename TResponseParser::TFutureResult TBatchRequestImpl::AddRequest(
    const TString& command,
    TNode parameters,
    TMaybe<TNode> input,
    TIntrusivePtr<TResponseParser> parser)
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

TFuture<ILockPtr> TBatchRequestImpl::Lock(
    const TTransactionId& transaction,
    const TYPath& path,
    ELockMode mode,
    const TLockOptions& options)
{
    return AddRequest<TLockResponseParser>(
        "lock",
        SerializeParamsForLock(transaction, path, mode, options),
        Nothing(),
        MakeIntrusive<TLockResponseParser>(options.Waitable_));
}

TFuture<TRichYPath> TBatchRequestImpl::CanonizeYPath(const TRichYPath& path)
{
    if (path.Path_.find_first_of("<>{}[]") != TString::npos) {
        return AddRequest<TCanonizeYPathResponseParser>(
            "parse_ypath",
            SerializeParamsForParseYPath(path),
            Nothing(),
            MakeIntrusive<TCanonizeYPathResponseParser>(path));
    } else {
        TRichYPath result = path;
        result.Path_ = AddPathPrefix(result.Path_);
        return NThreading::MakeFuture(result);
    }
}

void TBatchRequestImpl::FillParameterList(size_t maxSize, TNode* result, TInstant* nextTry) const
{
    Y_VERIFY(result);
    Y_VERIFY(nextTry);

    *nextTry = TInstant();
    maxSize = Min(maxSize, BatchItemList_.size());
    *result = TNode::CreateList();
    for (size_t i = 0; i < maxSize; ++i) {
        LOG_DEBUG("ExecuteBatch preparing: %s", ~RequestInfo(BatchItemList_[i].Parameters));
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
    const TClientPtr& client,
    TInstant now)
{
    TNode node = NodeFromYsonString(requestResult.Response);
    return ParseResponse(node, requestResult.RequestId, retryPolicy, retryBatch, client, now);
}

void TBatchRequestImpl::ParseResponse(
    TNode node,
    const TString& requestId,
    const IRetryPolicy& retryPolicy,
    TBatchRequestImpl* retryBatch,
    const TClientPtr& client,
    TInstant now)
{
    Y_VERIFY(retryBatch);

    TResponseContext responseContext;
    responseContext.Client = client;

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
                BatchItemList_[i].ResponseParser->SetResponse(std::move(outputIt->second), responseContext);
            } else {
                const auto errorIt = responseNode.find("error");
                if (errorIt == responseNode.end()) {
                    BatchItemList_[i].ResponseParser->SetResponse(Nothing(), responseContext);
                } else {
                    TErrorResponse error(400, requestId);
                    error.SetError(TError(errorIt->second));
                    if (auto curInterval = retryPolicy.GetRetryInterval(error)) {
                        LOG_INFO(
                            "Batch subrequest (%s) failed, will retry, error: %s",
                            ~RequestInfo(BatchItemList_[i].Parameters),
                            error.what());
                        retryBatch->AddRequest(TBatchItem(BatchItemList_[i], now + *curInterval));
                    } else {
                        LOG_ERROR(
                            "Batch subrequest (%s) failed, error: %s",
                            ~RequestInfo(BatchItemList_[i].Parameters),
                            error.what());
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

TBatchRequest::TBatchRequest(const TTransactionId& defaultTransaction, ::TIntrusivePtr<TClient> client)
    : DefaultTransaction_(defaultTransaction)
    , Impl_(MakeIntrusive<NDetail::TBatchRequestImpl>())
    , Client_(client)
{ }

TBatchRequest::TBatchRequest(NDetail::TBatchRequestImpl* impl)
    : Impl_(impl)
{ }

TBatchRequest::~TBatchRequest() = default;

IBatchRequestBase& TBatchRequest::WithTransaction(const TTransactionId& transactionId)
{
    if (!TmpWithTransaction_) {
        TmpWithTransaction_.Reset(new TBatchRequest(Impl_.Get()));
    }
    TmpWithTransaction_->DefaultTransaction_ = transactionId;
    return *TmpWithTransaction_;
}

TFuture<TNode> TBatchRequest::Get(
    const TYPath& path,
    const TGetOptions& options)
{
    return Impl_->Get(DefaultTransaction_, path, options);
}

TFuture<void> TBatchRequest::Set(const TYPath& path, const TNode& node)
{
    return Impl_->Set(DefaultTransaction_, path, node);
}

TFuture<TNode::TList> TBatchRequest::List(const TYPath& path, const TListOptions& options)
{
    return Impl_->List(DefaultTransaction_, path, options);
}

TFuture<bool> TBatchRequest::Exists(const TYPath& path)
{
    return Impl_->Exists(DefaultTransaction_, path);
}

TFuture<ILockPtr> TBatchRequest::Lock(
    const TYPath& path,
    ELockMode mode,
    const TLockOptions& options)
{
    return Impl_->Lock(DefaultTransaction_, path, mode, options);
}

TFuture<TLockId> TBatchRequest::Create(
    const TYPath& path,
    ENodeType type,
    const TCreateOptions& options)
{
    return Impl_->Create(DefaultTransaction_, path, type, options);
}

TFuture<void> TBatchRequest::Remove(
    const TYPath& path,
    const TRemoveOptions& options)
{
    return Impl_->Remove(DefaultTransaction_, path, options);
}

TFuture<TNodeId> TBatchRequest::Move(
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TMoveOptions& options)
{
    return Impl_->Move(DefaultTransaction_, sourcePath, destinationPath, options);
}

TFuture<TNodeId> TBatchRequest::Copy(
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TCopyOptions& options)
{
    return Impl_->Copy(DefaultTransaction_, sourcePath, destinationPath, options);
}

TFuture<TNodeId> TBatchRequest::Link(
    const TYPath& targetPath,
    const TYPath& linkPath,
    const TLinkOptions& options)
{
    return Impl_->Link(DefaultTransaction_, targetPath, linkPath, options);
}

TFuture<TRichYPath> TBatchRequest::CanonizeYPath(const TRichYPath& path)
{
    return Impl_->CanonizeYPath(path);
}

void TBatchRequest::ExecuteBatch(const TExecuteBatchOptions& options)
{
    if (Impl_->IsExecuted()) {
        ythrow yexception() << "Cannot execute batch request since it is alredy executed";
    }
    NDetail::TFinallyGuard g([&] {
        Impl_->MarkExecuted();
    });

    NDetail::TAttemptLimitedRetryPolicy retryPolicy(TConfig::Get()->RetryCount);

    const auto concurrency = options.Concurrency_.GetOrElse(50);
    const auto batchPartMaxSize = options.BatchPartMaxSize_.GetOrElse(concurrency * 5);

    while (Impl_->BatchSize()) {
        NDetail::TBatchRequestImpl retryBatch;

        while (Impl_->BatchSize()) {
            auto parameters = TNode::CreateMap();
            TInstant nextTry;
            Impl_->FillParameterList(batchPartMaxSize, &parameters["requests"], &nextTry);
            if (nextTry) {
                SleepUntil(nextTry);
            }
            parameters["concurrency"] = concurrency;
            auto body = NodeToYsonString(parameters);
            THttpHeader header("POST", "execute_batch");
            header.AddMutationId();
            NDetail::TResponseInfo result;
            try {
                result = RetryRequest(Client_->GetAuth(), header, body, retryPolicy);
            } catch (const yexception& e) {
                Impl_->SetErrorResult(std::current_exception());
                retryBatch.SetErrorResult(std::current_exception());
                throw;
            }
            Impl_->ParseResponse(std::move(result), retryPolicy, &retryBatch, Client_);
        }

        *Impl_ = std::move(retryBatch);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
