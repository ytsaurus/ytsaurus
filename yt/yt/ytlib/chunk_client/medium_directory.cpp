#include "medium_directory.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chunk_client/proto/medium_directory.pb.h>

namespace NYT::NChunkClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

const TMediumDescriptor* TMediumDirectory::FindByIndex(int index) const
{
    auto guard = ReaderGuard(SpinLock_);
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

const TMediumDescriptor* TMediumDirectory::FindByName(const std::string& name) const
{
    auto guard = ReaderGuard(SpinLock_);
    auto it = NameToDescriptor_.find(name);
    return it == NameToDescriptor_.end() ? nullptr : it->second;
}

const TMediumDescriptor* TMediumDirectory::GetByNameOrThrow(const std::string& name) const
{
    const auto* result = FindByName(name);
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

        YT_VERIFY(IndexToDescriptor_.emplace(descriptor.Index, descriptorPtr).second);
        YT_VERIFY(NameToDescriptor_.emplace(descriptor.Name, descriptorPtr).second);
    }
}

void TMediumDirectory::Clear()
{
    auto guard = WriterGuard(SpinLock_);
    IndexToDescriptor_.clear();
    NameToDescriptor_.clear();
    Descriptors_.clear();
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
                auto* descriptor = mediumDirectory->FindByIndex(mediumIndex);
                if (descriptor) {
                    fluent.Item(descriptor->Name).BeginMap()
                        .Item("index").Value(descriptor->Index)
                    .EndMap();
                }
            })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
