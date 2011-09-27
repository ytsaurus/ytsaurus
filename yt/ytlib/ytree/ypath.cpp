#include "ypath.h"

#include "../actions/action_util.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

void ChopYPathPrefix(
    const TYPath& path,
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
                YASSERT(false);
                break;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TYPath GetResolvedYPathPrefix(
    const TYPath& wholePath,
    const TYPath& unresolvedPath)
{
    int resolvedLength = static_cast<int>(wholePath.length()) - static_cast<int>(unresolvedPath.length());
    YASSERT(resolvedLength >= 0 && resolvedLength <= static_cast<int>(wholePath.length()));
    return TYPath(wholePath.begin(), wholePath.begin() + resolvedLength);
}

////////////////////////////////////////////////////////////////////////////////

TYPath ParseYPathRoot(const TYPath& path)
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

IYPathService* AsYPath(INode::TPtr node)
{
    return dynamic_cast<IYPathService*>(~node);
}

const IYPathService* AsYPath(INode::TConstPtr node)
{
    return const_cast<IYPathService*>(dynamic_cast<const IYPathService*>(~node));
}

////////////////////////////////////////////////////////////////////////////////

struct TYPathOperationState
{
    INode::TConstPtr CurrentNode;
    TYPath CurrentPath;
};

template <class T>
T ExecuteYPathOperation(
    INode::TConstPtr root,
    const TYPath& path,
    typename IParamFunc<TYPathOperationState, IYPathService::TResult<T> >::TPtr action,
    Stroka operationName)
{
    TYPathOperationState state;
    state.CurrentNode = root;
    state.CurrentPath = ParseYPathRoot(path);

    while (true) {
        auto result = action->Do(state);
        switch (result.Code) {
            case IYPathService::ECode::Done:
                return result.Value;

            case IYPathService::ECode::Recurse:
                state.CurrentNode = result.RecurseNode;
                state.CurrentPath = result.RecursePath;
                break;

            case IYPathService::ECode::Error:
                ythrow yexception() << Sprintf("Failed to %s YPath %s at %s: %s",
                    ~operationName,
                    ~Stroka(path).Quote(),
                    ~Stroka(GetResolvedYPathPrefix(path, state.CurrentPath)).Quote(),
                    ~result.ErrorMessage);

            default:
                YASSERT(false);
                break;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

IYPathService::TNavigateResult NavigateYPathAction(
    TYPathOperationState state)
{
    auto service = AsYPath(state.CurrentNode);
    if (service == NULL) {
        return IYPathService::TNavigateResult::CreateError("YPath is not supported");
    } else {
        return service->Navigate(state.CurrentPath);
    }
}

INode::TConstPtr NavigateYPath(
    INode::TConstPtr root,
    const TYPath& path)
{
    return ExecuteYPathOperation<INode::TConstPtr>(
        root,
        path,
        FromMethod(&NavigateYPathAction),
        "navigate");
}

////////////////////////////////////////////////////////////////////////////////

IYPathService::TGetResult GetYPathAction(
    TYPathOperationState state,
    IYsonConsumer* events)
{
    auto service = AsYPath(state.CurrentNode);
    if (service == NULL) {
        return IYPathService::TGetResult::CreateError("YPath is not supported");
    } else {
        return service->Get(state.CurrentPath, events);
    }
}

void GetYPath(
    INode::TConstPtr root,
    const TYPath& path,
    IYsonConsumer* events)
{
    ExecuteYPathOperation<TVoid>(
        root,
        path,
        FromMethod(&GetYPathAction, events),
        "get");
}

////////////////////////////////////////////////////////////////////////////////

IYPathService::TSetResult SetYPathAction(
    TYPathOperationState state,
    TYsonProducer::TPtr producer)
{
    auto service = AsYPath(state.CurrentNode->AsMutable());
    if (service == NULL) {
        return IYPathService::TSetResult::CreateError("YPath is not supported");
    } else {
        return service->Set(state.CurrentPath, producer);
    }
}

void SetYPath(
    INode::TConstPtr root,
    const TYPath& path,
    TYsonProducer::TPtr producer)
{
    ExecuteYPathOperation<TVoid>(
        root,
        path,
        FromMethod(&SetYPathAction, producer),
        "set");
}

////////////////////////////////////////////////////////////////////////////////

inline IYPathService::TRemoveResult RemoveYPathAction(
    TYPathOperationState state)
{
    auto service = AsYPath(state.CurrentNode->AsMutable());
    if (service == NULL) {
        return IYPathService::TSetResult::CreateError("YPath is not supported");
    } else {
        return service->Remove(state.CurrentPath);
    }
}

void RemoveYPath(
    INode::TConstPtr root,
    const TYPath& path)
{
    ExecuteYPathOperation<TVoid>(
        root,
        path,
        FromMethod(&RemoveYPathAction),
        "remove");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
