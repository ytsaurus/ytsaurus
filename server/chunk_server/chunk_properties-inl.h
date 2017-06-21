#pragma once
#ifndef CHUNK_PROPERTIES_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_properties.h"
#endif

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

inline TMediumChunkProperties::TMediumChunkProperties()
    : ReplicationFactor_(0)
    , DataPartsOnly_(false)
{ }

inline void TMediumChunkProperties::Clear()
{
    *this = TMediumChunkProperties();
}

inline int TMediumChunkProperties::GetReplicationFactor() const
{
    return ReplicationFactor_;
}

inline void TMediumChunkProperties::SetReplicationFactor(int replicationFactor)
{
    ReplicationFactor_ = replicationFactor;
}

inline bool TMediumChunkProperties::GetDataPartsOnly() const
{
    return DataPartsOnly_;
}

inline void TMediumChunkProperties::SetDataPartsOnly(bool dataPartsOnly)
{
    DataPartsOnly_ = dataPartsOnly;
}

inline TMediumChunkProperties::operator bool() const
{
    return GetReplicationFactor() != 0;
}

inline bool operator==(const TMediumChunkProperties& lhs, const TMediumChunkProperties& rhs)
{
    return lhs.GetReplicationFactor() == rhs.GetReplicationFactor() &&
        lhs.GetDataPartsOnly() == rhs.GetDataPartsOnly();
}

inline bool operator!=(const TMediumChunkProperties& lhs, const TMediumChunkProperties& rhs)
{
    return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////////////

inline TChunkProperties::TChunkProperties()
    : MediumChunkProperties_{}
    , Vital_(false)
{ }

inline TChunkProperties::const_iterator TChunkProperties::begin() const
{
    return MediumChunkProperties_.begin();
}

inline TChunkProperties::const_iterator TChunkProperties::end() const
{
    return MediumChunkProperties_.end();
}

inline TChunkProperties::const_iterator TChunkProperties::cbegin() const
{
    return begin();
}

inline TChunkProperties::const_iterator TChunkProperties::cend() const
{
    return end();
}

inline TChunkProperties::iterator TChunkProperties::begin()
{
    return MediumChunkProperties_.begin();
}

inline TChunkProperties::iterator TChunkProperties::end()
{
    return MediumChunkProperties_.end();
}

inline const TMediumChunkProperties& TChunkProperties::operator[](int mediumIndex) const
{
    return MediumChunkProperties_[mediumIndex];
}

inline TMediumChunkProperties& TChunkProperties::operator[](int mediumIndex)
{
    return MediumChunkProperties_[mediumIndex];
}

inline bool TChunkProperties::GetVital() const
{
    return Vital_;
}

inline void TChunkProperties::SetVital(bool vital)
{
    Vital_ = vital;
}

inline bool operator==(const TChunkProperties& lhs, const TChunkProperties& rhs)
{
    if (&lhs == &rhs)
        return true;

    return (lhs.GetVital() == rhs.GetVital()) &&
        std::equal(lhs.begin(), lhs.end(), rhs.begin());
}

inline bool operator!=(const TChunkProperties& lhs, const TChunkProperties& rhs)
{
    return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
