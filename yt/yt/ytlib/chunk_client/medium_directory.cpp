#include "medium_directory.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chunk_client/proto/medium_directory.pb.h>

namespace NYT::NChunkClient {

using namespace NConcurrency;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TMediumDescriptorPtr TMediumDirectory::FindByIndex(int index) const
{
    auto guard = ReaderGuard(SpinLock_);
    auto it = IndexToDescriptor_.find(index);
    return it == IndexToDescriptor_.end() ? nullptr : it->second;
}

TMediumDescriptorPtr TMediumDirectory::GetByIndexOrThrow(int index) const
{
    auto result = FindByIndex(index);
    if (!result) {
        THROW_ERROR_EXCEPTION("No such medium %v", index);
    }
    return result;
}

TMediumDescriptorPtr TMediumDirectory::FindByName(const std::string& name) const
{
    auto guard = ReaderGuard(SpinLock_);
    auto it = NameToDescriptor_.find(name);
    return it == NameToDescriptor_.end() ? nullptr : it->second->second;
}

TMediumDescriptorPtr TMediumDirectory::GetByNameOrThrow(const std::string& name) const
{
    auto result = FindByName(name);
    if (!result) {
        THROW_ERROR_EXCEPTION("No such medium %Qv", name);
    }
    return result;
}

std::vector<int> TMediumDirectory::GetMediumIndexes() const
{
    auto guard = ReaderGuard(SpinLock_);
    std::vector<int> result;
    result.reserve(IndexToDescriptor_.size());
    for (const auto& [index, descriptor] : IndexToDescriptor_) {
        result.push_back(index);
    }
    return result;
}

void TMediumDirectory::LoadFrom(const NProto::TMediumDirectory& protoDirectory)
{
    auto guard = WriterGuard(SpinLock_);
    auto oldIndexToDescriptor = std::move(IndexToDescriptor_);
    IndexToDescriptor_.clear();
    NameToDescriptor_.clear();
    for (const auto& protoItem : protoDirectory.medium_descriptors()) {
        auto descriptor = FromProto<TMediumDescriptorPtr>(protoItem);

        // Let's keep the same pointer if the medium configuration did not change since the descriptor
        // sometimes caches things, e.g. S3 client in S3 medium descriptor.
        auto oldIt = oldIndexToDescriptor.find(descriptor->GetIndex());
        if (oldIt != oldIndexToDescriptor.end() && *oldIt->second == *descriptor) {
            descriptor = std::move(oldIt->second);
        }
        EmplaceOrCrash(IndexToDescriptor_, descriptor->GetIndex(), descriptor);
        EmplaceOrCrash(NameToDescriptor_, descriptor->Name(), descriptor);
    }
}

void TMediumDirectory::Clear()
{
    auto guard = WriterGuard(SpinLock_);
    IndexToDescriptor_.clear();
    NameToDescriptor_.clear();
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TMediumDirectoryPtr& mediumDirectory, NYson::IYsonConsumer* consumer)
{
    if (!mediumDirectory) {
        NYTree::BuildYsonFluently(consumer)
            .BeginMap()
            .EndMap();
        return;
    }
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .DoFor(mediumDirectory->GetMediumIndexes(), [&] (auto fluent, int mediumIndex) {
                auto descriptor = mediumDirectory->FindByIndex(mediumIndex);
                if (descriptor) {
                    fluent.Item(descriptor->Name()).BeginMap()
                        .Item("index").Value(descriptor->GetIndex())
                    .EndMap();
                }
            })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
