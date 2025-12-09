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

NS3::TObjectDescriptor TS3MediumDescriptor::GetChunkPlacement(TChunkId chunkId, const std::string& sourceUri) const
{
    if (sourceUri.empty()) {
        TString key = Config_->Prefix;
        if (!key.empty() && !key.EndsWith("/")) {
            key.push_back('/');
        }
        key += Format("chunk-data/%02x/%02x/%v", chunkId.ReversedParts8[1], chunkId.ReversedParts8[0], chunkId);
        if (Config_->Bucket.Empty()) {
            THROW_ERROR_EXCEPTION("Cannot place chunks into S3 medium %Qv without a configured bucket", Name_);
        }
        return NS3::TObjectDescriptor(Config_->Bucket, key);
    }

    return NS3::TObjectDescriptor::FromUri(sourceUri);
};

NS3::TObjectDescriptor TS3MediumDescriptor::GetChunkMetaPlacement(TChunkId chunkId, bool externallyAttached) const
{
    if (externallyAttached) {
        TString key = Config_->Prefix;
        if (!key.empty() && !key.EndsWith("/")) {
            key.push_back('/');
        }
        key += Format("generated-chunk-data/%02x/%02x/%v%v", chunkId.ReversedParts8[1], chunkId.ReversedParts8[0], chunkId, ChunkMetaSuffix);
        if (Config_->Bucket.Empty()) {
            THROW_ERROR_EXCEPTION("Cannot place chunks into S3 medium %Qv without a configured bucket", Name_);
        }
        return NS3::TObjectDescriptor(Config_->Bucket, key);
    }
    auto chunkPlacement = GetChunkPlacement(chunkId, /*sourceUri*/ {});
    return NS3::TObjectDescriptor(
        chunkPlacement.Bucket(),
        chunkPlacement.Key() + ChunkMetaSuffix);
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

NS3::IClientPtr TS3MediumDescriptor::CreateClient(const TS3MediumConfigPtr& mediumConfig)
{
    // TODO(achulkov2): Figure out proper invoker to use.
    auto invoker = TDispatcher::Get()->GetWriterInvoker();

    auto clientConfig = New<NS3::TS3ClientConfig>();
    clientConfig->Url = mediumConfig->Url;
    clientConfig->Region = mediumConfig->Region;
    clientConfig->Bucket = mediumConfig->Bucket;

    auto client = NS3::CreateClient(
        std::move(clientConfig),
        NS3::CreateStaticCredentialProvider(mediumConfig->AccessKeyId, mediumConfig->SecretAccessKey),
        NYT::NBus::TTcpDispatcher::Get()->GetXferPoller(),
        invoker);

    auto backoffOptions = TExponentialBackoffOptions{
        .InvocationCount = 5,
        .MinBackoff = TDuration::MilliSeconds(100),
        .MaxBackoff = TDuration::Seconds(1),
        .BackoffMultiplier = 1.5,
    };

    return NS3::CreateRetryingClient(
        std::move(client),
        std::move(backoffOptions),
        std::move(invoker));
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
            .Item("name").Value(mediumDescriptor->GetName())
            .Item("index").Value(mediumDescriptor->GetIndex())
            .Item("priority").Value(mediumDescriptor->GetPriority())
            .Item("id").Value(mediumDescriptor->GetId())
        .EndMap();

    // TODO(achulkov2): [PLater] Serialize type-specific descriptor.
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
