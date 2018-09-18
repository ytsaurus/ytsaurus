#include "medium_directory.h"

#include <yt/core/misc/error.h>

#include <yt/ytlib/chunk_client/medium_directory.pb.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

bool TMediumDescriptor::operator==(const TMediumDescriptor& other) const
{
    return
        Name == other.Name &&
        Index == other.Index &&
        Priority == other.Priority;
}

bool TMediumDescriptor::operator!=(const TMediumDescriptor& other) const
{
    return !(*this == other);
}

////////////////////////////////////////////////////////////////////////////////

const TMediumDescriptor* TMediumDirectory::FindByIndex(int index) const
{
    NConcurrency::TReaderGuard guard(SpinLock_);
    auto it = IndexToDescriptor_.find(index);
    return it == IndexToDescriptor_.end() ? nullptr : it->second;
}

const TMediumDescriptor* TMediumDirectory::GetByIndexOrThrow(int index) const
{
    const auto* result = FindByIndex(index);
    if (!result) {
        THROW_ERROR_EXCEPTION("No such medium %v", index);
    }
    return result;
}

const TMediumDescriptor* TMediumDirectory::FindByName(const TString& name) const
{
    NConcurrency::TReaderGuard guard(SpinLock_);
    auto it = NameToDescriptor_.find(name);
    return it == NameToDescriptor_.end() ? nullptr : it->second;
}

const TMediumDescriptor* TMediumDirectory::GetByNameOrThrow(const TString& name) const
{
    const auto* result = FindByName(name);
    if (!result) {
        THROW_ERROR_EXCEPTION("No such medium %Qv", name);
    }
    return result;
}

void TMediumDirectory::LoadFrom(const NProto::TMediumDirectory& protoDirectory)
{
    NConcurrency::TWriterGuard guard(SpinLock_);
    auto oldIndexToDescriptor = std::move(IndexToDescriptor_);
    IndexToDescriptor_.clear();
    NameToDescriptor_.clear();
    for (const auto& protoItem : protoDirectory.items()) {
        TMediumDescriptor descriptor;
        descriptor.Name = protoItem.name();
        descriptor.Index = protoItem.index();
        descriptor.Priority = protoItem.priority();

        const TMediumDescriptor* descriptorPtr;
        auto it = oldIndexToDescriptor.find(descriptor.Index);
        if (it == oldIndexToDescriptor.end() || *it->second != descriptor) {
            auto descriptorHolder = std::make_unique<TMediumDescriptor>(descriptor);
            descriptorPtr = descriptorHolder.get();
            Descriptors_.emplace_back(std::move(descriptorHolder));
        } else {
            descriptorPtr = it->second;
        }

        YCHECK(IndexToDescriptor_.emplace(descriptor.Index, descriptorPtr).second);
        YCHECK(NameToDescriptor_.emplace(descriptor.Name, descriptorPtr).second);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
