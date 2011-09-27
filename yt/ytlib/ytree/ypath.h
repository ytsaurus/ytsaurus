#pragma once

#include "common.h"
#include "ytree.h"
#include "yson_events.h"

#include "../actions/action.h"

// TODO: remove once impl is moved to cpp
#include "../actions/action_util.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

inline void ChopYPathPrefix(
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

//inline void ChopYPathSuffix(
//    const TYPath& path,
//    Stroka* suffix,
//    TYPath* headPath)
//{
//    size_t index = path.find_last_of("/@");
//    if (index == TYPath::npos) {
//        *suffix = path;
//        *headPath = TYPath(path.begin(), static_cast<size_t>(0));
//    } else {
//        switch (path[index]) {
//            case '/':
//                *suffix = Stroka(path.begin() + index + 1, path.end());
//                *headPath = TYPath(path.begin(), path.begin() + index);
//                break;
//
//            case '@':
//                *suffix = Stroka(path.begin() + index + 1, path.end());
//                *headPath = TYPath(path.begin(), path.begin() + index + 1);
//                break;
//
//            default:
//                YASSERT(false);
//                break;
//        }
//    }
//}
//

////////////////////////////////////////////////////////////////////////////////

inline TYPath GetResolvedYPathPrefix(
    const TYPath& wholePath,
    const TYPath& unresolvedPath)
{
    int resolvedLength = static_cast<int>(wholePath.length()) - static_cast<int>(unresolvedPath.length());
    YASSERT(resolvedLength >= 0 && resolvedLength <= static_cast<int>(wholePath.length()));
    return TYPath(wholePath.begin(), wholePath.begin() + resolvedLength);
}

////////////////////////////////////////////////////////////////////////////////

inline TYPath ParseYPathRoot(const TYPath& path)
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

struct TYPathOperationState
{
    INode::TConstPtr CurrentNode;
    TYPath CurrentPath;
};

template <class T>
inline T ExecuteYPathOperation(
    INode::TConstPtr root,
    const TYPath& path,
    typename IParamFunc<TYPathOperationState, INode::TYPathResult<T> >::TPtr action,
    Stroka operationName)
{
    TYPathOperationState state;
    state.CurrentNode = root;
    state.CurrentPath = ParseYPathRoot(path);

    while (true) {
        auto result = action->Do(state);
        switch (result.Code) {
            case INode::EYPathCode::Done:
                return result.Value;

            case INode::EYPathCode::Recurse:
                state.CurrentNode = result.RecurseNode;
                state.CurrentPath = result.RecursePath;
                break;

            case INode::EYPathCode::Error:
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

inline INode::TNavigateResult NavigateYPathAction(
    TYPathOperationState state)
{
    return state.CurrentNode->YPathNavigate(state.CurrentPath);
}

inline INode::TConstPtr NavigateYPath(
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

inline INode::TGetResult GetYPathAction(
    TYPathOperationState state,
    IYsonConsumer* events)
{
    return state.CurrentNode->YPathGet(state.CurrentPath, events);
}

inline void GetYPath(
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

inline INode::TSetResult SetYPathAction(
    TYPathOperationState state,
    TYsonProducer::TPtr producer)
{
    return state.CurrentNode->AsMutable()->YPathSet(state.CurrentPath, producer);
}

inline void SetYPath(
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

inline INode::TRemoveResult RemoveYPathAction(
    TYPathOperationState state)
{
    return state.CurrentNode->AsMutable()->YPathRemove(state.CurrentPath);
}

inline void RemoveYPath(
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
