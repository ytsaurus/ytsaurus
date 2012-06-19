#include "file_helpers.h"

#include <ytlib/misc/fs.h>
#include <util/folder/dirut.h>

namespace NYT {
namespace NMetaState {

void Move(Stroka source, Stroka destination)
{
    if (isexist(~destination)) {
        YCHECK(NFS::Remove(~destination));
    }
    YCHECK(NFS::Rename(~source, ~destination));
}

}} // namespace NYT::NMetaState
