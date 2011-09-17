#pragma once

#include "common.h"
#include "ytree.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYPathNavigator
{
public:
    TYPathNavigator(INode::TConstPtr root)
        : Root(root)
    { }

    INode::TConstPtr Navigate(const TYPath& path)
    {
        if (path.Empty()) {
            ythrow yexception() << "YPath cannot be empty, use \"/\" to denote the root";
        }

        if (path[0] != '/') {
            ythrow yexception() << "YPath must start with \"'\"";
        }

        TYPath currentPath = path;
        INode::TConstPtr currentNode = Root;

        while (!currentPath.Empty()) {
            INode::TConstPtr nextNode;
            TYPath nextPath;
            if (!currentNode->Navigate(currentPath, &nextNode, &nextPath)) {
                int charsParsed = currentPath.begin() - path.begin();
                YASSERT(charsParsed >= 0 && charsParsed <= static_cast<int>(path.length()));
                Stroka pathParsed(path.SubString(0, charsParsed));
                // TODO: escaping
                ythrow yexception() << Sprintf("Unable to resolve an YPath %s at %s",
                    ~currentPath,
                    ~pathParsed);
            }
            currentNode = nextNode;
            currentPath = nextPath;
        }

        return NULL;
    }

private:
    INode::TConstPtr Root;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
