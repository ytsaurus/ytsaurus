#include "medium_directory.h"

#include <yt/core/misc/error.h>

#include <yt/ytlib/chunk_client/medium_directory.pb.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

Stroka TMediumDirectory::GetNameByIndex(int index) const
{
    NConcurrency::TReaderGuard guard(SpinLock_);
    auto it = IndexToName_.find(index);
    if (it == IndexToName_.end()) {
        THROW_ERROR_EXCEPTION("No such medium %v", index);
    }
    return it->second;
}

int TMediumDirectory::GetIndexByName(const Stroka& name) const
{
    NConcurrency::TReaderGuard guard(SpinLock_);
    auto it = NameToIndex_.find(name);
    if (it == NameToIndex_.end()) {
        THROW_ERROR_EXCEPTION("No such medium %Qv", name);
    }
    return it->second;
}

void TMediumDirectory::UpdateDirectory(const NProto::TMediumDirectory& protoDirectory)
{
    NConcurrency::TWriterGuard guard(SpinLock_);
    IndexToName_.clear();
    NameToIndex_.clear();
    for (const auto& protoItem : protoDirectory.items()) {
        YCHECK(IndexToName_.emplace(protoItem.index(), protoItem.name()).second);
        YCHECK(NameToIndex_.emplace(protoItem.name(), protoItem.index()).second);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
