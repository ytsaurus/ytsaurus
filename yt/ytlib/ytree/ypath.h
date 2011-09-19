#pragma once

#include "common.h"
#include "ytree.h"

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

inline void ChopYPathSuffix(
    const TYPath& path,
    Stroka* suffix,
    TYPath* headPath)
{
    size_t index = path.find_last_of("/@");
    if (index == TYPath::npos) {
        *suffix = path;
        *headPath = TYPath(path.begin(), static_cast<size_t>(0));
    } else {
        switch (path[index]) {
            case '/':
                *suffix = Stroka(path.begin() + index + 1, path.end());
                *headPath = TYPath(path.begin(), path.begin() + index);
                break;

            case '@':
                *suffix = Stroka(path.begin() + index + 1, path.end());
                *headPath = TYPath(path.begin(), path.begin() + index + 1);
                break;

            default:
                YASSERT(false);
                break;
        }
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
        ythrow yexception() << "YPath must start with \"/\"";
    }

    auto currentNode = root;
    auto currentPath = TYPath(path.begin() + 1, path.end());
    INode::TConstPtr currentTailNode;
    TYPath currentTailPath;
    while (!currentPath.empty() &&
           currentNode->YPathNavigate(currentPath, &currentTailNode, &currentTailPath)) {
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

////////////////////////////////////////////////////////////////////////////////

inline void SetYPath(
    INode::TConstPtr root,
    const TYPath& path,
    INode::TPtr value)
{
    INode::TConstPtr tailNode;
    TYPath headPath;
    TYPath tailPath;
    NavigateYPath(root, path, &tailNode, &headPath, &tailPath);
    
    if (tailPath.empty()) {
        tailNode->AsMutable()->YPathAssign(value);
    } else {
        Stroka suffix;
        TYPath headTailPath;
        ChopYPathSuffix(tailPath, &suffix, &headTailPath);
        
        INode::TPtr appendNode;
        if (headTailPath.empty()) {
            appendNode = tailNode->AsMutable();
            if (appendNode->GetType() != ENodeType::Map) {
                ythrow yexception() << Sprintf("Cannot append a child to node %s of type %s",
                    ~Stroka(headPath).Quote(),
                    ~tailNode->GetType().ToString());
            }
        } else {
            tailNode->AsMutable()->YPathForce(headTailPath, &appendNode);
            YASSERT(appendNode->GetType() == ENodeType::Map);
        }

        appendNode->AsMap()->AddChild(value, suffix);
    }
}

////////////////////////////////////////////////////////////////////////////////

inline void RemoveYPath(
    INode::TConstPtr root,
    const TYPath& path)
{
    auto node = GetYPath(root, path);
    node->AsMutable()->YPathRemove();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
