#include "medium_directory.h"
#include "medium_descriptor.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chunk_client/proto/medium_directory.pb.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NChunkClient {

using NYT::ToProto;
using NYT::FromProto;

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NYson;
using namespace NYTree;

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

TMediumDescriptorPtr TMediumDirectory::FindByName(const TString& name) const
{
    auto guard = ReaderGuard(SpinLock_);
    auto it = NameToDescriptor_.find(name);
    return it == NameToDescriptor_.end() ? nullptr : it->second->second;
}

TMediumDescriptorPtr TMediumDirectory::GetByNameOrThrow(const TString& name) const
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

TString TMediumDirectory::GetMediumName(int index) const
{
    auto descriptor = FindByIndex(index);
    return descriptor ? descriptor->GetName() : TString("unknown");
}

void TMediumDirectory::LoadFrom(const NProto::TMediumDirectory& protoDirectory)
{
    auto guard = WriterGuard(SpinLock_);

    auto oldIndexToDescriptor = std::move(IndexToDescriptor_);
    IndexToDescriptor_.clear();
    NameToDescriptor_.clear();
    for (const auto& protoMediumDescriptor : protoDirectory.medium_descriptors()) {
        auto descriptor = TMediumDescriptor::CreateFromProto(protoMediumDescriptor);

        // Let's keep the same pointer if the medium configuration did not change since the descriptor
        // sometimes caches things, e.g. S3 client in S3 medium descriptor.
        auto oldIt = oldIndexToDescriptor.find(descriptor->GetIndex());
        if (oldIt != oldIndexToDescriptor.end() && *oldIt->second == *descriptor) {
            descriptor = std::move(oldIt->second);
        }

        auto [it, inserted] = IndexToDescriptor_.emplace(descriptor->GetIndex(), descriptor);
        YT_VERIFY(inserted);
        EmplaceOrCrash(NameToDescriptor_, descriptor->GetName(), it);
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
        BuildYsonFluently(consumer)
            .BeginMap()
            .EndMap();
        return;
    }
    BuildYsonFluently(consumer)
        .BeginMap()
            .DoFor(mediumDirectory->GetMediumIndexes(), [&] (auto fluent, int mediumIndex) {
                auto descriptor = mediumDirectory->FindByIndex(mediumIndex);
                if (descriptor) {
                    fluent.Item(descriptor->GetName()).Value(descriptor);
                }
            })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
