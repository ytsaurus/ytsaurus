#pragma once

#include "common.h"
#include "ytree.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

inline void ChopYPathToken(
    const TYPath& path,
    Stroka* token,
    TYPath* tailPath)
{
    size_t index = path.find_first_of("/@");
    if (index == TYPath::npos) {
        *token = path;
        *tailPath = TYPath(path.end(), static_cast<size_t>(0));
    } else {
        *token = Stroka(path.begin(), index);
        *tailPath = TYPath(path.begin() + index + 1, path.end());
    }
}

////////////////////////////////////////////////////////////////////////////////

inline void NavigateYPath(
    INode::TConstPtr root,
    const TYPath& path,
    INode::TConstPtr* tailNode,
    TYPath* headPath,
    TYPath* tailPath)
{
    if (path.empty()) {
        ythrow yexception() << "YPath cannot be empty, use \"/\" to denote the root";
    }

    if (path[0] != '/') {
        ythrow yexception() << "YPath must start with \"'\"";
    }

    auto currentNode = root;
    auto currentPath = TYPath(path.begin() + 1, path.end());
    INode::TConstPtr currentTailNode;
    TYPath currentTailPath;
    while (!currentPath.empty() &&
           currentNode->NavigateYPath(currentPath, &currentTailNode, &currentTailPath)) {
        currentNode = currentTailNode;
        currentPath = currentTailPath;
    }

    *tailNode = currentNode;
    *tailPath = currentPath;

    int resolvedLength = static_cast<int>(path.length()) - static_cast<int>(tailPath->length());
    YASSERT(resolvedLength >= 0 && resolvedLength <= static_cast<int>(path.length()));
    *headPath = TYPath(path.begin(), path.begin() + resolvedLength);
}

////////////////////////////////////////////////////////////////////////////////

inline INode::TConstPtr GetYPath(
    INode::TConstPtr root,
    const TYPath& path)
{
    INode::TConstPtr tailNode;
    TYPath headPath;
    TYPath tailPath;
    NavigateYPath(root, path, &tailNode, &headPath, &tailPath);
    if (!tailPath.empty()) {
        ythrow yexception() << Sprintf("Cannot resolve YPath %s at %s",
            ~Stroka(tailPath).Quote(),
            ~Stroka(headPath).Quote());
    }
    return tailNode;
}

inline INode::TConstPtr GetYPath(
    INode::TConstPtr root,
    const TYPath& path)
{
    INode::TConstPtr tailNode;
    TYPath headPath;
    TYPath tailPath;
    NavigateYPath(root, path, &tailNode, &headPath, &tailPath);
    if (!tailPath.empty()) {
        ythrow yexception() << Sprintf("Cannot resolve YPath %s at %s",
            ~Stroka(tailPath).Quote(),
            ~Stroka(headPath).Quote());
    }
    return tailNode;
}

////////////////////////////////////////////////////////////////////////////////

inline void SetYPath(
    INodeFactory* factory,
    INode::TPtr root,
    const TYPath& path,
    INode::TPtr value)
{
    root->SetYPath(factory, path, value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
