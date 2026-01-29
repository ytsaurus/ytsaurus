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

bool TMediumDescriptor::IsOffshore() const
{
    return !IsDomestic();
}

bool TMediumDescriptor::operator==(const TMediumDescriptor& other) const
{
    return IsDomestic() == other.IsDomestic() && Name_ == other.Name_ && Index_ == other.Index_ && Priority_ == other.Priority_;
}

////////////////////////////////////////////////////////////////////////////////

TDomesticMediumDescriptor::TDomesticMediumDescriptor(std::string name, int index, int priority)
    : TMediumDescriptor(std::move(name), index, priority)
{ }

TDomesticMediumDescriptor::TDomesticMediumDescriptor(std::string name)
    : TMediumDescriptor(std::move(name), GenericMediumIndex, -1)
{ }

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

bool TS3MediumDescriptor::IsDomestic() const
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TMediumDescriptorPtr& mediumDescriptor, NYson::IYsonConsumer* consumer)
{
    if (!mediumDescriptor) {
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

void ToProto(NProto::TMediumDirectory::TMediumDescriptor* protoMediumDescriptor, const TMediumDescriptorPtr& mediumDescriptor)
{
    protoMediumDescriptor->Clear();

    protoMediumDescriptor->set_index(mediumDescriptor->GetIndex());
    protoMediumDescriptor->set_name(mediumDescriptor->Name());
    protoMediumDescriptor->set_priority(mediumDescriptor->GetPriority());

    if (mediumDescriptor->IsOffshore()) {
        auto* s3MediumDescriptor = protoMediumDescriptor->mutable_s3_medium_descriptor();
        s3MediumDescriptor->set_config(ConvertToYsonString(mediumDescriptor->As<TS3MediumDescriptor>()->GetConfig()).AsStringBuf());
    } else {
        // NB: Domestic medium descriptor is empty.
        protoMediumDescriptor->mutable_domestic_medium_descriptor();
    }
}

void FromProto(TMediumDescriptorPtr* mediumDescriptor, const NProto::TMediumDirectory::TMediumDescriptor& protoMediumDescriptor)
{
    auto name = FromProto<std::string>(protoMediumDescriptor.name());
    auto index = FromProto<int>(protoMediumDescriptor.index());
    auto priority = FromProto<int>(protoMediumDescriptor.priority());

    switch (protoMediumDescriptor.type_specific_descriptor_case()) {
        case NProto::TMediumDirectory::TMediumDescriptor::TypeSpecificDescriptorCase::kDomesticMediumDescriptor: {
            *mediumDescriptor = New<TDomesticMediumDescriptor>(std::move(name), index, priority);
            break;
        }
        case NProto::TMediumDirectory::TMediumDescriptor::TypeSpecificDescriptorCase::kS3MediumDescriptor: {
            auto config = ConvertTo<TS3MediumConfigPtr>(TYsonString(protoMediumDescriptor.s3_medium_descriptor().config()));
            *mediumDescriptor = New<TS3MediumDescriptor>(std::move(name), index, priority, std::move(config));
            break;
        }
        // COMPAT(cherepashka, achulkov2): Remove this case and fallthrough to throwing an exception once all masters start filling this field.
        case NProto::TMediumDirectory::TMediumDescriptor::TypeSpecificDescriptorCase::TYPE_SPECIFIC_DESCRIPTOR_NOT_SET: {
            *mediumDescriptor = New<TDomesticMediumDescriptor>(std::move(name), index, priority);
            break;
        }
        default:
            THROW_ERROR_EXCEPTION(
                "Medium descriptor proto contains unknown medium type %v",
                static_cast<i64>(protoMediumDescriptor.type_specific_descriptor_case()));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
