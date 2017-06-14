#include "chunk_properties.h"

#include "chunk_manager.h"
#include "medium.h"

#include <yt/server/cell_master/automaton.h>
#include <yt/core/misc/serialize.h>

namespace NYT {
namespace NChunkServer {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TMediumChunkProperties& TMediumChunkProperties::operator|=(const TMediumChunkProperties& rhs)
{
    if (this == &rhs)
        return *this;

    SetReplicationFactor(std::max(GetReplicationFactor(), rhs.GetReplicationFactor()));
    SetDataPartsOnly(GetDataPartsOnly() && rhs.GetDataPartsOnly());

    return *this;
}

void TMediumChunkProperties::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, ReplicationFactor_);
    Save(context, DataPartsOnly_);
}

void TMediumChunkProperties::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    ReplicationFactor_ = Load<decltype(ReplicationFactor_)>(context);
    DataPartsOnly_ = Load<decltype(DataPartsOnly_)>(context);
}

void FormatValue(TStringBuilder* builder, const TMediumChunkProperties& properties, const TStringBuf& /*spec*/)
{
    builder->AppendFormat("{ReplicationFactor: %v, DataPartsOnly: %v}",
        properties.GetReplicationFactor(),
        properties.GetDataPartsOnly());
}

TString ToString(const TMediumChunkProperties& properties)
{
    return ToStringViaBuilder(properties);
}

////////////////////////////////////////////////////////////////////////////////

void TChunkProperties::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, MediumChunkProperties_);
    Save(context, Vital_);
}

void TChunkProperties::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    Load(context, MediumChunkProperties_);
    Load(context, Vital_);
}

TChunkProperties& TChunkProperties::operator|=(const TChunkProperties& rhs)
{
    if (this == &rhs) {
        return *this;
    }

    SetVital(GetVital() || rhs.GetVital());

    for (int i = 0; i < MaxMediumCount; ++i) {
        (*this)[i] |= rhs[i];
    }

    return *this;
}

bool TChunkProperties::IsValid() const
{
    for (const auto& mediumProps : MediumChunkProperties_) {
        if (mediumProps && !mediumProps.GetDataPartsOnly()) {
            // At least one medium has complete data.
            return true;
        }
    }

    return false;
}

void FormatValue(TStringBuilder* builder, const TChunkProperties& properties, const TStringBuf& /*spec*/)
{
    builder->AppendFormat("{Vital: %v, Media: {", properties.GetVital());

    // We want to accompany medium properties with their indexes.
    using TIndexPropertiesPair = std::pair<int, TMediumChunkProperties>;

    SmallVector<TIndexPropertiesPair, MaxMediumCount> filteredProperties;
    int mediumIndex = 0;
    for (const auto& mediumProperties : properties) {
        if (mediumProperties) {
            filteredProperties.emplace_back(mediumIndex, mediumProperties);
        }
        ++mediumIndex;
    }

    JoinToString(builder, filteredProperties.begin(), filteredProperties.end(),
        [&] (TStringBuilder* aBuilder, const TIndexPropertiesPair& pair) {
            aBuilder->AppendFormat("%v: %v", pair.first, pair.second);
        });

    builder->AppendString("}}");
}

TString ToString(const TChunkProperties& properties)
{
    return ToStringViaBuilder(properties);
}

////////////////////////////////////////////////////////////////////////////////

TSerializableChunkProperties::TSerializableChunkProperties(
    const TChunkProperties& properties,
    const TChunkManagerPtr& chunkManager)
{
    for (int mediumIndex = 0; mediumIndex < MaxMediumCount; ++mediumIndex) {
        const auto& mediumProperties = properties[mediumIndex];
        if (mediumProperties) {
            auto* medium = chunkManager->GetMediumByIndex(mediumIndex);

            TMediumProperties resultMediumProps;
            resultMediumProps.ReplicationFactor = mediumProperties.GetReplicationFactor();
            resultMediumProps.DataPartsOnly = mediumProperties.GetDataPartsOnly();
            YCHECK(resultMediumProps.ReplicationFactor != 0);

            YCHECK(MediumProperties_.emplace(medium->GetName(), resultMediumProps).second);
        }
    }
}

void TSerializableChunkProperties::ToChunkProperties(
    TChunkProperties* properties,
    const TChunkManagerPtr& chunkManager)
{
    for (auto& mediumProperties : *properties) {
        mediumProperties.Clear();
    }

    for (const auto& pair : MediumProperties_) {
        auto* medium = chunkManager->GetMediumByNameOrThrow(pair.first);
        auto mediumIndex = medium->GetIndex();
        auto& mediumProperties = (*properties)[mediumIndex];
        mediumProperties.SetReplicationFactor(pair.second.ReplicationFactor);
        mediumProperties.SetDataPartsOnly(pair.second.DataPartsOnly);
    }
}

void TSerializableChunkProperties::Serialize(NYson::IYsonConsumer* consumer) const
{
    BuildYsonFluently(consumer)
        .Value(MediumProperties_);
}

void TSerializableChunkProperties::Deserialize(INodePtr node)
{
    YCHECK(node);

    MediumProperties_ = ConvertTo<std::map<TString, TMediumProperties>>(node);
}

void Serialize(const TSerializableChunkProperties& serializer, NYson::IYsonConsumer* consumer)
{
    serializer.Serialize(consumer);
}

void Deserialize(TSerializableChunkProperties& serializer, INodePtr node)
{
    serializer.Deserialize(node);
}

void Serialize(const TSerializableChunkProperties::TMediumProperties& properties, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("replication_factor").Value(properties.ReplicationFactor)
            .Item("data_parts_only").Value(properties.DataPartsOnly)
        .EndMap();
}

void Deserialize(TSerializableChunkProperties::TMediumProperties& properties, INodePtr node)
{
    auto map = node->AsMap();
    properties.ReplicationFactor = map->GetChild("replication_factor")->AsInt64()->GetValue();
    properties.DataPartsOnly = map->GetChild("data_parts_only")->AsBoolean()->GetValue();
}

////////////////////////////////////////////////////////////////////////////////

void ValidateReplicationFactor(int replicationFactor)
{
    if (replicationFactor != 0 && // Zero is a special - and permitted - case.
        (replicationFactor < NChunkClient::MinReplicationFactor ||
         replicationFactor > NChunkClient::MaxReplicationFactor))
    {
        THROW_ERROR_EXCEPTION("Replication factor %v is out of range [%v,%v]",
            replicationFactor,
            NChunkClient::MinReplicationFactor,
            NChunkClient::MaxReplicationFactor);
    }
}

void ValidateChunkProperties(
    const TChunkManagerPtr& chunkManager,
    const TChunkProperties& properties,
    int primaryMediumIndex)
{
    if (!properties.IsValid()) {
        THROW_ERROR_EXCEPTION(
            "At least one medium should store replicas (including parity parts); "
            "configuring otherwise would result in a data loss");
    }

    for (int index = 0; index < MaxMediumCount; ++index) {
        const auto* medium = chunkManager->FindMediumByIndex(index);
        if (!medium) {
            continue;
        }

        const auto& mediumProperties = properties[index];
        if (mediumProperties && medium->GetCache()) {
            THROW_ERROR_EXCEPTION("Cache medium %Qv cannot be configured explicitly",
                medium->GetName());
        }
    }

    const auto* primaryMedium = chunkManager->GetMediumByIndex(primaryMediumIndex);
    const auto& primaryMediumProperties = properties[primaryMediumIndex];
    if (!primaryMediumProperties) {
        THROW_ERROR_EXCEPTION("Medium %Qv is not configured and cannot be made primary",
            primaryMedium->GetName());
    }
    if (primaryMediumProperties.GetDataPartsOnly()) {
        THROW_ERROR_EXCEPTION("Medium %Qv stores no parity parts and cannot be made primary",
            primaryMedium->GetName());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
