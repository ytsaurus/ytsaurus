#include "detailed_master_memory.h"

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/ytree/serialize.h>

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/lib/security_server/proto/security_manager.pb.h>

#include <library/cpp/yt/misc/cast.h>

namespace NYT::NSecurityServer {

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

i64 TDetailedMasterMemory::operator[] (EMasterMemoryType type) const
{
    return DetailedMasterMemory_[type];
}

i64& TDetailedMasterMemory::operator[] (EMasterMemoryType type)
{
    return DetailedMasterMemory_[type];
}

TDetailedMasterMemory& TDetailedMasterMemory::operator += (const TDetailedMasterMemory& other)
{
    for (auto type : TEnumTraits<EMasterMemoryType>::GetDomainValues()) {
        DetailedMasterMemory_[type] += other[type];
    }
    return *this;
}

TDetailedMasterMemory TDetailedMasterMemory::operator + (const TDetailedMasterMemory& other) const
{
    auto result = *this;
    result += other;
    return result;
}

TDetailedMasterMemory& TDetailedMasterMemory::operator -= (const TDetailedMasterMemory& other)
{
    for (auto type : TEnumTraits<EMasterMemoryType>::GetDomainValues()) {
        DetailedMasterMemory_[type] -= other[type];
    }
    return *this;
}

TDetailedMasterMemory TDetailedMasterMemory::operator - (const TDetailedMasterMemory& other) const
{
    auto result = *this;
    result -= other;
    return result;
}

TDetailedMasterMemory& TDetailedMasterMemory::operator *= (i64 other)
{
    for (auto type : TEnumTraits<EMasterMemoryType>::GetDomainValues()) {
        DetailedMasterMemory_[type] *= other;
    }
    return *this;
}

TDetailedMasterMemory TDetailedMasterMemory::operator * (i64 other) const
{
    auto result = *this;
    result *= other;
    return result;
}

TDetailedMasterMemory TDetailedMasterMemory::operator - () const
{
    auto result = *this;
    result *= -1;
    return result;
}

bool TDetailedMasterMemory::operator == (const TDetailedMasterMemory& other) const
{
    for (auto type : TEnumTraits<EMasterMemoryType>::GetDomainValues()) {
        if (DetailedMasterMemory_[type] != other[type]) {
            return false;
        }
    }
    return true;
}

bool TDetailedMasterMemory::IsNegative() const
{
    for (auto type : TEnumTraits<EMasterMemoryType>::GetDomainValues()) {
        if (DetailedMasterMemory_[type] < 0) {
            return true;
        }
    }
    return false;
}

bool TDetailedMasterMemory::IsZero() const
{
    for (auto type : TEnumTraits<EMasterMemoryType>::GetDomainValues()) {
        if (DetailedMasterMemory_[type] != 0) {
            return false;
        }
    }
    return true;
}

i64 TDetailedMasterMemory::GetTotal() const
{
    i64 sum = 0;
    for (auto type : TEnumTraits<EMasterMemoryType>::GetDomainValues()) {
        sum += DetailedMasterMemory_[type];
    }
    return sum;
}

void TDetailedMasterMemory::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, DetailedMasterMemory_);
}

void TDetailedMasterMemory::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    Load(context, DetailedMasterMemory_);
}

void TDetailedMasterMemory::Save(NCypressServer::TBeginCopyContext& context) const
{
    using NYT::Save;
    Save(context, DetailedMasterMemory_);
}

void TDetailedMasterMemory::Load(NCypressServer::TEndCopyContext& context)
{
    using NYT::Load;
    Load(context, DetailedMasterMemory_);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TDetailedMasterMemory* protoDetailedMasterMemory,
    const TDetailedMasterMemory& detailedMasterMemory)
{
    for (auto masterMemoryType : TEnumTraits<EMasterMemoryType>::GetDomainValues()) {
        auto* entry = protoDetailedMasterMemory->add_entries();
        entry->set_master_memory_type(static_cast<int>(masterMemoryType));
        entry->set_master_memory_usage(detailedMasterMemory[masterMemoryType]);
    }
}

void FromProto(
    TDetailedMasterMemory* detailedMasterMemory,
    const NProto::TDetailedMasterMemory& protoDetailedMasterMemory)
{
    for (const auto& detailedMasterMemoryEntry : protoDetailedMasterMemory.entries()) {
        auto type = FromProto<EMasterMemoryType>(detailedMasterMemoryEntry.master_memory_type());
        auto usage = detailedMasterMemoryEntry.master_memory_usage();
        (*detailedMasterMemory)[type] = usage;
    }
}

void Serialize(const TDetailedMasterMemory& detailedMasterMemory, NYson::IYsonConsumer* consumer)
{
    NYTree::Serialize(detailedMasterMemory.DetailedMasterMemory_, consumer);
}

void Deserialize(TDetailedMasterMemory& detailedMasterMemory, NYTree::INodePtr node)
{
    NYTree::Deserialize(detailedMasterMemory.DetailedMasterMemory_, node);
}

void FormatValue(TStringBuilderBase* builder, const TDetailedMasterMemory& detailedMasterMemory, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v", detailedMasterMemory.DetailedMasterMemory_);
}

TString ToString(const TDetailedMasterMemory& detailedMasterMemory)
{
    return ToStringViaBuilder(detailedMasterMemory);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
