#include "stdafx.h"
#include "file_helpers.h"

#include <ytlib/misc/fs.h>
#include <util/folder/dirut.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

void CheckedMoveFile(const Stroka& source, const Stroka& destination)
{
    if (isexist(~destination)) {
        YCHECK(NFS::Remove(~destination));
    }
    YCHECK(NFS::Rename(~source, ~destination));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
