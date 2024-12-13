#include "medium_directory.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chunk_client/proto/medium_directory.pb.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>

#include <yt/yt/library/s3/client.h>

#include <yt/yt/core/bus/tcp/dispatcher.h>

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

static constexpr auto& Logger = ChunkClientLogger;

////////////////////////////////////////////////////////////////////////////////

TMediumDescriptor::TMediumDescriptor(TString name, int index, int priority, TGuid id)
    : Name_(std::move(name))
    , Index_(index)
    , Priority_(priority)
    , Id_(id)
{ }

void TMediumDescriptor::FillProto(NProto::TMediumDirectory::TMediumDescriptor* protoMediumDescriptor) const
{
    protoMediumDescriptor->set_name(Name_);
    protoMediumDescriptor->set_index(Index_);
    protoMediumDescriptor->set_priority(Priority_);
    ToProto(protoMediumDescriptor->mutable_id(), Id_);
}

bool TMediumDescriptor::Equals(const TMediumDescriptor& other) const
{
    return Name_ == other.Name_ && Index_ == other.Index_ && Priority_ == other.Priority_ && Id_ == other.Id_;
}

void TMediumDescriptor::LoadFrom(const NProto::TMediumDirectory::TMediumDescriptor& protoMediumDescriptor)
{
    Name_ = protoMediumDescriptor.name();
    Index_ = protoMediumDescriptor.index();
    Priority_ = protoMediumDescriptor.priority();
    // COMPAT(achulkov2): Start throwing an exception here once all masters start filling the id.
    if (protoMediumDescriptor.has_id()) {
        Id_ = FromProto<TGuid>(protoMediumDescriptor.id());
    }
}

TMediumDescriptorPtr TMediumDescriptor::CreateFromProto(const NProto::TMediumDirectory::TMediumDescriptor& protoMediumDescriptor)
{
    TMediumDescriptorPtr descriptor;

    switch (protoMediumDescriptor.type_specific_descriptor_case()) {
        case NProto::TMediumDirectory::TMediumDescriptor::TypeSpecificDescriptorCase::kDomesticMediumDescriptor: {
            descriptor = New<TDomesticMediumDescriptor>();
            break;
        }
        case NProto::TMediumDirectory::TMediumDescriptor::TypeSpecificDescriptorCase::kS3MediumDescriptor: {
            descriptor = New<TS3MediumDescriptor>();
            break;
        }
        // COMPAT(achulkov2): Remove this case and fallthrough to throwing an exception once all masters start filling this field.
        case NProto::TMediumDirectory::TMediumDescriptor::TypeSpecificDescriptorCase::TYPE_SPECIFIC_DESCRIPTOR_NOT_SET: {
            descriptor = New<TDomesticMediumDescriptor>();
            break;
        }
        default:
            THROW_ERROR_EXCEPTION(
                "Medium descriptor proto contains unknown medium type %v",
                static_cast<i64>(protoMediumDescriptor.type_specific_descriptor_case()));
    }

    descriptor->LoadFrom(protoMediumDescriptor);
    return descriptor;
}

bool TMediumDescriptor::IsOffshore() const
{
    return !IsDomestic();
}

bool TMediumDescriptor::operator==(const TMediumDescriptor& other) const
{
    return typeid(this) == typeid(other) && Equals(other);
}

////////////////////////////////////////////////////////////////////////////////

TDomesticMediumDescriptor::TDomesticMediumDescriptor(TString name, int index, int priority, TGuid id)
    : TMediumDescriptor(std::move(name), index, priority, id)
{ }

TDomesticMediumDescriptor::TDomesticMediumDescriptor(TString name)
    : TMediumDescriptor(std::move(name), GenericMediumIndex, -1, NullObjectId)
{ }

bool TDomesticMediumDescriptor::Equals(const TMediumDescriptor& other) const
{
    if (!TMediumDescriptor::Equals(other)) {
        return false;
    }

    if (other.As<TDomesticMediumDescriptor>()) {
        return true;
    }

    return false;
}

void TDomesticMediumDescriptor::FillProto(NProto::TMediumDirectory::TMediumDescriptor* protoMediumDescriptor) const
{
    TMediumDescriptor::FillProto(protoMediumDescriptor);

    // Nothing to be filled, just creating an empty message.
    Y_UNUSED(protoMediumDescriptor->mutable_domestic_medium_descriptor());
}

void TDomesticMediumDescriptor::LoadFrom(const NProto::TMediumDirectory::TMediumDescriptor& protoMediumDescriptor)
{
    TMediumDescriptor::LoadFrom(protoMediumDescriptor);

    // Nothing to be loaded.
}

bool TDomesticMediumDescriptor::IsDomestic() const
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TS3MediumDescriptor::TS3MediumDescriptor(TString name, int index, int priority, TGuid id, TS3MediumConfigPtr config)
    : TMediumDescriptor(std::move(name), index, priority, id)
    , Config_(std::move(config))
    , Client_(CreateClient(Config_))
{ }

TS3MediumDescriptor::TS3ObjectPlacement TS3MediumDescriptor::GetChunkPlacement(TChunkId chunkId) const
{
    return {
        .Bucket = Config_->Bucket,
        .Key = Format("chunk-data/%v", chunkId)
    };
};

TS3MediumDescriptor::TS3ObjectPlacement TS3MediumDescriptor::GetChunkMetaPlacement(TChunkId chunkId) const
{
    auto metaPlacement = GetChunkPlacement(chunkId);
    metaPlacement.Key += ChunkMetaSuffix;
    return metaPlacement;
};

bool TS3MediumDescriptor::Equals(const TMediumDescriptor& other) const
{
    if (!TMediumDescriptor::Equals(other)) {
        return false;
    }

    if (const auto otherS3Descriptor = other.As<TS3MediumDescriptor>()) {
        return *Config_ == *otherS3Descriptor->Config_;
    }

    return false;
}

void TS3MediumDescriptor::FillProto(NProto::TMediumDirectory::TMediumDescriptor* protoMediumDescriptor) const
{
    TMediumDescriptor::FillProto(protoMediumDescriptor);

    ToProto(protoMediumDescriptor->mutable_s3_medium_descriptor()->mutable_config(), ConvertToYsonString(Config_).ToString());
}

void TS3MediumDescriptor::LoadFrom(const NProto::TMediumDirectory::TMediumDescriptor& protoMediumDescriptor)
{
    TMediumDescriptor::LoadFrom(protoMediumDescriptor);

    Config_ = ConvertTo<TS3MediumConfigPtr>(TYsonString(protoMediumDescriptor.s3_medium_descriptor().config()));
    Client_ = CreateClient(Config_);
}

bool TS3MediumDescriptor::IsDomestic() const
{
    return false;
}

NS3::IClientPtr TS3MediumDescriptor::CreateClient(const TS3MediumConfigPtr& config)
{
    return NS3::CreateClient(
        ConvertTo<NS3::TS3ClientConfigPtr>(NYson::ConvertToYsonString(config)),
        NYT::NBus::TTcpDispatcher::Get()->GetXferPoller(),
        // TODO(achulkov2): [PLater] Figure out proper invoker to use.
        TDispatcher::Get()->GetWriterInvoker());
}

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

    YT_LOG_DEBUG("KEK Loading medium directory (DebugString: %v)", protoDirectory.DebugString());

    auto oldIndexToDescriptor = std::move(IndexToDescriptor_);
    IndexToDescriptor_.clear();
    NameToDescriptor_.clear();
    for (const auto& protoMediumDescriptor : protoDirectory.medium_descriptors()) {
        auto descriptor = TMediumDescriptor::CreateFromProto(protoMediumDescriptor);

        YT_LOG_DEBUG("KEK Adding medium to medium directory (Index: %v, Name: %v)", descriptor->GetIndex(), descriptor->GetName());

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
                    fluent.Item(descriptor->GetName()).BeginMap()
                        .Item("index").Value(descriptor->GetIndex())
                    .EndMap();
                }
            })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
