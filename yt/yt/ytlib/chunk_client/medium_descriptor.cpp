#include "medium_descriptor.h"
#include "private.h"

#include <yt/yt/ytlib/chunk_client/dispatcher.h>

#include <yt/yt/library/s3/client.h>

#include <yt/yt/core/bus/tcp/dispatcher.h>

namespace NYT::NChunkClient {

using NYT::ToProto;
using NYT::FromProto;

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TMediumDescriptor::TMediumDescriptor(std::string name, int index, int priority)
    : Name_(std::move(name))
    , Index_(index)
    , Priority_(priority)
{ }

bool TMediumDescriptor::Equals(const TMediumDescriptor& other) const
{
    return Name_ == other.Name_ && Index_ == other.Index_ && Priority_ == other.Priority_;
}

void TMediumDescriptor::LoadFrom(const NProto::TMediumDirectory::TMediumDescriptor& protoMediumDescriptor)
{
    Name_ = protoMediumDescriptor.name();
    Index_ = protoMediumDescriptor.index();
    Priority_ = protoMediumDescriptor.priority();
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
        // COMPAT(cherepashka, achulkov2): Remove this case and fallthrough to throwing an exception once all masters start filling this field.
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

TDomesticMediumDescriptor::TDomesticMediumDescriptor(std::string name, int index, int priority)
    : TMediumDescriptor(std::move(name), index, priority)
{ }

TDomesticMediumDescriptor::TDomesticMediumDescriptor(std::string name)
    : TMediumDescriptor(std::move(name), GenericMediumIndex, -1)
{ }

bool TDomesticMediumDescriptor::Equals(const TMediumDescriptor& other) const
{
    if (!TMediumDescriptor::Equals(other)) {
        return false;
    }

    return other.As<TDomesticMediumDescriptor>() != nullptr;
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

TS3MediumDescriptor::TS3MediumDescriptor(std::string name, int index, int priority, TS3MediumConfigPtr config)
    : TMediumDescriptor(std::move(name), index, priority)
    , Config_(std::move(config))
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

void TS3MediumDescriptor::LoadFrom(const NProto::TMediumDirectory::TMediumDescriptor& protoMediumDescriptor)
{
    TMediumDescriptor::LoadFrom(protoMediumDescriptor);

    Config_ = ConvertTo<TS3MediumConfigPtr>(TYsonString(protoMediumDescriptor.s3_medium_descriptor().config()));
}

bool TS3MediumDescriptor::IsDomestic() const
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TMediumDescriptorPtr& mediumDescriptor, NYson::IYsonConsumer* consumer)
{
    if (!mediumDescriptor) {
        BuildYsonFluently(consumer)
            .BeginMap()
            .EndMap();
        return;
    }
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("name").Value(mediumDescriptor->Name())
            .Item("index").Value(mediumDescriptor->GetIndex())
            .Item("priority").Value(mediumDescriptor->GetPriority())
            .Item("offshore").Value(mediumDescriptor->IsOffshore())
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
