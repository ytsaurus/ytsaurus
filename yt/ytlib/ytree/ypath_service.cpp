#include "stdafx.h"
#include "ypath_service.h"
#include "tree_builder.h"
#include "ephemeral.h"

#include "../actions/action_util.h"
#include "../rpc/server_detail.h"

namespace NYT {
namespace NYTree {

using namespace NRpc;
using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

IYPathService::TPtr IYPathService::FromNode(INode* node)
{
    YASSERT(node != NULL);
    auto* service = dynamic_cast<IYPathService*>(node);
    if (service == NULL) {
        ythrow yexception() << "Node does not support YPath";
    }
    return service;
}

IYPathService::TPtr IYPathService::FromProducer(TYsonProducer* producer)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    producer->Do(~builder);
    return FromNode(~builder->EndTree());
}

////////////////////////////////////////////////////////////////////////////////

void ChopYPathPrefix(
    TYPath path,
    Stroka* prefix,
    TYPath* tailPath)
{
    size_t index = path.find_first_of("/@");
    if (index == TYPath::npos) {
        *prefix = path;
        *tailPath = TYPath(path.end(), static_cast<size_t>(0));
    } else {
        switch (path[index]) {
            case '/':
                *prefix = Stroka(path.begin(), index);
                *tailPath =
                    index == path.length() - 1
                    ? TYPath(path.begin() + index, path.end())
                    : TYPath(path.begin() + index + 1, path.end());
                break;

            case '@':
                *prefix = Stroka(path.begin(), index);
                *tailPath = TYPath(path.begin() + index, path.end());
                break;

            default:
                YUNREACHABLE();
        }
    }
}

TYPath GetResolvedYPathPrefix(
    TYPath wholePath,
    TYPath unresolvedPath)
{
    int resolvedLength = static_cast<int>(wholePath.length()) - static_cast<int>(unresolvedPath.length());
    YASSERT(resolvedLength >= 0 && resolvedLength <= static_cast<int>(wholePath.length()));
    return TYPath(wholePath.begin(), wholePath.begin() + resolvedLength);
}


TYPath ParseYPathRoot(TYPath path)
{
    if (path.empty()) {
        ythrow yexception() << "YPath cannot be empty, use \"/\" to denote the root";
    }

    if (path[0] != '/') {
        ythrow yexception() << "YPath must start with \"/\"";
    }

    return TYPath(path.begin() + 1, path.end());
}

////////////////////////////////////////////////////////////////////////////////

//struct TYPathOperationState
//{
//    IYPathService::TPtr CurrentService;
//    TYPath CurrentPath;
//};
//
//template <class T>
//T ExecuteYPathVerb(
//    IYPathService::TPtr rootService,
//    TYPath path,
//    typename IParamFunc<TYPathOperationState, IYPathService::TResult<T> >::TPtr verb,
//    Stroka verbName)
//{
//    TYPathOperationState state;
//    state.CurrentService = rootService;
//    state.CurrentPath = ParseYPathRoot(path);
//
//    while (true) {
//        IYPathService::TResult<T> result;
//        try {
//            result = verb->Do(state);
//        } catch (const TYTreeException& ex) {
//            // TODO: ypath escaping and normalization
//            ythrow TYTreeException() << Sprintf("Failed to execute YPath operation (Verb: %s, Path: %s, ErrorPath: %s, ErrorMessage: %s)",
//                ~verbName,
//                ~Stroka(path),
//                ~Stroka(GetResolvedYPathPrefix(path, state.CurrentPath)),
//                ex.what());
//        }
//        switch (result.Code) {
//            case IYPathService::ECode::Done:
//                return result.Value;
//
//            case IYPathService::ECode::Recurse:
//                state.CurrentService = result.RecurseService;
//                state.CurrentPath = result.RecursePath;
//                break;
//
//            default:
//                YUNREACHABLE();
//        }
//    }
//}

////////////////////////////////////////////////////////////////////////////////

//INode::TPtr NavigateYPath(
//    IYPathService::TPtr rootService,
//    TYPath path)
//{
//    return ExecuteYPathVerb<INode::TPtr>(
//        rootService,
//        path,
//        FromFunctor([&] (TYPathOperationState state) -> IYPathService::TNavigateResult
//            {
//                return state.CurrentService->Navigate(state.CurrentPath);
//            }),
//        "navigate");
//}
//
//void GetYPath(
//    IYPathService::TPtr rootService,
//    TYPath path,
//    IYsonConsumer* consumer)
//{
//    ExecuteYPathVerb<TVoid>(
//        rootService,
//        path,
//        FromFunctor([&] (TYPathOperationState state) -> IYPathService::TGetResult
//            {
//                return state.CurrentService->Get(state.CurrentPath, consumer);
//            }),
//        "get");
//}
//
//INode::TPtr SetYPath(
//    IYPathService::TPtr rootService,
//    TYPath path,
//    TYsonProducer::TPtr producer)
//{
//    return ExecuteYPathVerb<INode::TPtr>(
//        rootService,
//        path,
//        FromFunctor([&] (TYPathOperationState state) -> IYPathService::TSetResult
//            {
//                return state.CurrentService->Set(state.CurrentPath, producer);
//            }),
//        "set");
//}
//
//void RemoveYPath(
//    IYPathService::TPtr rootService,
//    TYPath path)
//{
//    ExecuteYPathVerb<TVoid>(
//        rootService,
//        path,
//        FromFunctor([&] (TYPathOperationState state) -> IYPathService::TRemoveResult
//            {
//                return state.CurrentService->Remove(state.CurrentPath);
//            }),
//        "remove");
//}
//
//void LockYPath(
//    IYPathService::TPtr rootService,
//    TYPath path)
//{
//    ExecuteYPathVerb<TVoid>(
//        rootService,
//        path,
//        FromFunctor([&] (TYPathOperationState state) -> IYPathService::TLockResult
//            {
//                return state.CurrentService->Lock(state.CurrentPath);
//            }),
//        "lock");
//}
//} 


class TServiceContext
    : public TServiceContextBase
{
public:
    TServiceContext(
        const Stroka& path,
        const Stroka& verb,
        NBus::IMessage* requestMessage,
        TYPathResponseHandler* responseHandler,
        const Stroka& loggingCategory)
        : TServiceContextBase(TRequestId(), path, verb, requestMessage)
        , ResponseHandler(responseHandler)
        , Logger(loggingCategory)
    { }

protected:
    TYPathResponseHandler::TPtr ResponseHandler;
    NLog::TLogger Logger;

    virtual void DoReply(const TError& error, IMessage* responseMessage)
    {
        TYPathResponseHandlerParam response;
        response.Message = responseMessage;
        response.Error = error;
        ResponseHandler->Do(response);
    }

    virtual void LogRequest()
    {
        Stroka str;
        AppendInfo(str, RequestInfo);
        LOG_DEBUG("%s %s <- %s",
            ~Verb,
            ~Path,
            ~str);
    }

    virtual void LogResponse(const TError& error)
    {
        Stroka str;
        AppendInfo(str, Sprintf("Error: %s", ~error.ToString()));
        AppendInfo(str, ResponseInfo);
        LOG_DEBUG("%s %s -> %s",
            ~Verb,
            ~Path,
            ~str);
    }

    virtual void LogException(const Stroka& message)
    {
        Stroka str;
        AppendInfo(str, Sprintf("Path: %s", ~Path));
        AppendInfo(str, Sprintf("Verb: %s", ~Verb));
        AppendInfo(str, ResponseInfo);
        LOG_FATAL("Unhandled exception in YPath service method (%s): %s",
            ~str,
            ~message);
    }

};

void NavigateYPath(
    IYPathService* rootService,
    TYPath path,
    bool mustExist,
    IYPathService::TPtr* tailService,
    TYPath* tailPath)
{
    IYPathService::TPtr currentService = rootService;
    auto currentPath = path;
    while (true) {
        IYPathService::TNavigateResult result;
        try {
            result = currentService->Navigate(currentPath, mustExist);
        } catch (...) {
            ythrow yexception() << Sprintf("Error during YPath navigation (Path: %s, ResolvedPath: %s): %s",
                ~path,
                ~GetResolvedYPathPrefix(path, currentPath),
                ~CurrentExceptionMessage());
        }

        if (result.IsHere()) {
            *tailService = result.GetService();
            *tailPath = result.GetPath();
            return;
        }

        currentService = result.GetService();
        currentPath = result.GetPath();
    }
}

IYPathService::TPtr NavigateYPath(
    IYPathService* rootService,
    TYPath path)
{
    IYPathService::TPtr tailService;
    TYPath tailPath;
    NavigateYPath(rootService, path, true, &tailService, &tailPath);
    return tailService;
}

void InvokeYPathVerb(
    IYPathService* service,
    TYPath path,
    const Stroka& verb,
    NRpc::IServiceContext* outerContext,
    const Stroka& loggingCategory,
    TYPathResponseHandler* responseHandler)
{
    auto innerContext = UnwrapYPathRequest(
        outerContext,
        path,
        verb,
        loggingCategory,
        responseHandler);
 
    service->Invoke(~innerContext);
}

NRpc::IServiceContext::TPtr UnwrapYPathRequest(
    NRpc::IServiceContext* outerContext,
    TYPath path,
    const Stroka& verb,
    const Stroka& loggingCategory,
    TYPathResponseHandler* responseHandler)
{
    auto attachments = outerContext->GetRequestAttachments();

    yvector<TSharedRef> parts;
    parts.reserve(1 + attachments.ysize());
    // Put dummy RPC header part.
    parts.push_back(TSharedRef());
    // Put the encapsulated message.
    NStl::copy(
        attachments.begin(),
        attachments.end(),
        NStl::back_inserter(attachments));

    auto innerMessage = CreateMessageFromParts(parts);

    return New<TServiceContext>(
        path,
        verb,
        ~innerMessage,
        responseHandler,
        loggingCategory);
}

void WrapYPathResponse(
    NRpc::IServiceContext* outerContext,
    NBus::IMessage* responseMessage)
{
    outerContext->SetResponseAttachments(responseMessage->GetParts());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
