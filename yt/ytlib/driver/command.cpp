#include "stdafx.h"
#include "command.h"

namespace NYT {
namespace NDriver {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TUntypedCommandBase::TUntypedCommandBase(ICommandHost* host)
    : Host(host)
{ }

void TUntypedCommandBase::PreprocessYPath(TYPath* path)
{
    *path = Host->PreprocessYPath(*path);
}

void TUntypedCommandBase::PreprocessYPaths(std::vector<NYTree::TYPath>* paths)
{
    for (int index = 0; index < static_cast<int>(paths->size()); ++index) {
        (*paths)[index] = Host->PreprocessYPath((*paths)[index]);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
