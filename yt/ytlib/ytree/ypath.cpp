#include "stdafx.h"
#include "ypath.h"

#include "../actions/action_util.h"

namespace NYT {
namespace NYTree {

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
                *tailPath = TYPath(path.begin() + index + 1, path.end());
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

////////////////////////////////////////////////////////////////////////////////

TYPath GetResolvedYPathPrefix(
    TYPath wholePath,
    TYPath unresolvedPath)
{
    int resolvedLength = static_cast<int>(wholePath.length()) - static_cast<int>(unresolvedPath.length());
    YASSERT(resolvedLength >= 0 && resolvedLength <= static_cast<int>(wholePath.length()));
    return TYPath(wholePath.begin(), wholePath.begin() + resolvedLength);
}

////////////////////////////////////////////////////////////////////////////////

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

IYPathService::TPtr AsYPath(INode::TPtr node)
{
    YASSERT(~node != NULL);
    auto* service = dynamic_cast<IYPathService*>(~node);
    YASSERT(service != NULL);
    return service;
}

////////////////////////////////////////////////////////////////////////////////

struct TYPathOperationState
{
    IYPathService::TPtr CurrentService;
    TYPath CurrentPath;
};

template <class T>
T ExecuteYPathVerb(
    IYPathService::TPtr rootService,
    TYPath path,
    typename IParamFunc<TYPathOperationState, IYPathService::TResult<T> >::TPtr verb,
    Stroka verbName)
{
    TYPathOperationState state;
    state.CurrentService = rootService;
    state.CurrentPath = ParseYPathRoot(path);

    while (true) {
        IYPathService::TResult<T> result;
        try {
            result = verb->Do(state);
        } catch (const TYTreeException& ex) {
            // TODO: ypath escaping and normalization
            ythrow TYTreeException() << Sprintf("Failed to execute YPath operation (Verb: %s, Path: %s, ErrorPath: %s, ErrorMessage: %s)",
                ~verbName,
                ~Stroka(path),
                ~Stroka(GetResolvedYPathPrefix(path, state.CurrentPath)),
                ex.what());
        }
        switch (result.Code) {
            case IYPathService::ECode::Done:
                return result.Value;

            case IYPathService::ECode::Recurse:
                state.CurrentService = result.RecurseService;
                state.CurrentPath = result.RecursePath;
                break;

            default:
                YUNREACHABLE();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

INode::TPtr NavigateYPath(
    IYPathService::TPtr rootService,
    TYPath path)
{
    return ExecuteYPathVerb<INode::TPtr>(
        rootService,
        path,
        FromFunctor([&] (TYPathOperationState state) -> IYPathService::TNavigateResult
            {
                return state.CurrentService->Navigate(state.CurrentPath);
            }),
        "navigate");
}

void GetYPath(
    IYPathService::TPtr rootService,
    TYPath path,
    IYsonConsumer* consumer)
{
    ExecuteYPathVerb<TVoid>(
        rootService,
        path,
        FromFunctor([&] (TYPathOperationState state) -> IYPathService::TGetResult
            {
                return state.CurrentService->Get(state.CurrentPath, consumer);
            }),
        "get");
}

void SetYPath(
    IYPathService::TPtr rootService,
    TYPath path,
    TYsonProducer::TPtr producer)
{
    ExecuteYPathVerb<TVoid>(
        rootService,
        path,
        FromFunctor([&] (TYPathOperationState state) -> IYPathService::TSetResult
            {
                return state.CurrentService->Set(state.CurrentPath, producer);
            }),
        "set");
}

void RemoveYPath(
    IYPathService::TPtr rootService,
    TYPath path)
{
    ExecuteYPathVerb<TVoid>(
        rootService,
        path,
        FromFunctor([&] (TYPathOperationState state) -> IYPathService::TRemoveResult
            {
                return state.CurrentService->Remove(state.CurrentPath);
            }),
        "remove");
}

void LockYPath(
    IYPathService::TPtr rootService,
    TYPath path)
{
    ExecuteYPathVerb<TVoid>(
        rootService,
        path,
        FromFunctor([&] (TYPathOperationState state) -> IYPathService::TLockResult
            {
                return state.CurrentService->Lock(state.CurrentPath);
            }),
        "lock");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
