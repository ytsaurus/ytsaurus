#pragma once

#include <yt/server/cell_master/public.h>

#include <yt/server/chunk_server/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/core/misc/property.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/yson_serializable.h>

#include <array>

#include "chunk_manager.h"
#include "medium.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

void ValidateReplicationFactor(int replicationFactor);
void ValidateMediumIndex(int mediumIndex);

////////////////////////////////////////////////////////////////////////////////

//! Represents a medium-specific properties of a chunk or a chunk owner.
struct TMediumChunkProperties
{
protected:
    ui8 ReplicationFactor_ : 7;
    // For certain media, it may be helpful to be able to store just the data
    // parts of erasure coded chunks.
    bool DataPartsOnly_ : 1;

public:
    TMediumChunkProperties();

    void Clear();

    int GetReplicationFactor() const;
    // Throws if #replicationFactor is out of range.
    void SetReplicationFactorOrThrow(int replicationFactor);

    bool GetDataPartsOnly() const;
    void SetDataPartsOnly(bool dataPartsOnly);

    //! Returns true iff replication factor is not zero.
    /*!
     * Semantically, this means that the owner of these properties (a chunk or
     * a chunk owner) has this medium 'enabled'.
     */
    explicit operator bool() const;

    //! Combines this object with #rhs by MAXing replication factors and ANDing
    //! 'data parts only' flags.
    TMediumChunkProperties& operator|=(const TMediumChunkProperties& rhs);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

static_assert(sizeof(TMediumChunkProperties) == 1, "sizeof(TMediumChunkProperties) != 1");

bool operator==(const TMediumChunkProperties& lhs, const TMediumChunkProperties& rhs);
bool operator!=(const TMediumChunkProperties& lhs, const TMediumChunkProperties& rhs);

Stroka ToString(const TMediumChunkProperties& properties);

////////////////////////////////////////////////////////////////////////////////

class TChunkProperties
{
public:
    using TMediumChunkPropertiesArray = TPerMediumArray<TMediumChunkProperties>;

    using const_iterator = typename TMediumChunkPropertiesArray::const_iterator;
    using iterator = typename TMediumChunkPropertiesArray::iterator;

public:
    //! Constructs an 'empty' set of properties.
    /*!
     * THE RESULTING STATE IS UNSAFE. It has replication factors set to zero and
     * thus doesn't represent a sensible set of defaults for a chunk owner.
     * Neither it is suitable for combining with other sets via #operator|=().
     * In particular, 'data parts only' flags are combined by ANDing, and the
     * default value (of false) would affect the end result of combining.
     */
    TChunkProperties();

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    const_iterator begin() const;
    const_iterator end() const;
    const_iterator cbegin() const;
    const_iterator cend() const;
    iterator begin();
    iterator end();

    // Throw if #mediumIndex is out of range.
    const TMediumChunkProperties& operator[](int mediumIndex) const;
    TMediumChunkProperties& operator[](int mediumIndex);

    bool GetVital() const;
    void SetVital(bool vital);

    //! Combines these properties with #rhs by ORing 'vital' flags and combining
    //! properties for each medium (see #TMediumChunkProperties::operator|=()).
    TChunkProperties& operator|=(const TChunkProperties& rhs);

    //! Returns true iff these properties would not result in a data loss
    //! (i.e. on at least on medium, replication factor is non-zero and
    //! 'data parts only' flag is set to false).
    bool Validate() const Y_WARN_UNUSED_RESULT;
    //! Throws iff #Validate() returns false.
    void ValidateOrThrow() const;

private:
    explicit TChunkProperties(bool safe);

private:
    TMediumChunkPropertiesArray MediumChunkProperties_;
    bool Vital_;

    static_assert(
        sizeof(MediumChunkProperties_) == MaxMediumCount * sizeof(TMediumChunkProperties),
        "sizeof(MediumChunkProperties_) != MaxMediumCount * sizeof(TMediumChunkProperties)");
};

bool operator==(const TChunkProperties& lhs, const TChunkProperties& rhs);
bool operator!=(const TChunkProperties& lhs, const TChunkProperties& rhs);

Stroka ToString(const TChunkProperties& properties);

////////////////////////////////////////////////////////////////////////////////

//! A helper class for yson-serializing media-specific parts of #TChunkProperties
//! (i.e. everything except the 'vital' flag).
/*!
 * [De]serializing those classes directly is not an option since that would
 * require mapping medium indexes to their names, which can't be done without
 * #TChunkManager.
 */
class TMediaSerializer
{
public:
    struct TMediumProperties
    {
        int ReplicationFactor = 0;
        bool DataPartsOnly = false;
    };

public:
    TMediaSerializer() = default;
    TMediaSerializer(const TChunkProperties& props, const TChunkManagerPtr& chunkManager);

    void ToChunkPropertiesOrThrow(
        TChunkProperties* props,
        const TChunkManagerPtr& chunkManager);

    void Serialize(NYson::IYsonConsumer* consumer) const;
    void Deserialize(NYTree::INodePtr node);

private:
    // Media are sorted by name at serialization. this is by no means required,
    // we're just being nice here.
    std::map<Stroka, TMediumProperties> MediumProperties_;
};

void Serialize(const TMediaSerializer& serializer, NYson::IYsonConsumer* consumer);
void Deserialize(TMediaSerializer& serializer, NYTree::INodePtr node);

void Serialize(const TMediaSerializer::TMediumProperties& serializer, NYson::IYsonConsumer* consumer);
void Deserialize(TMediaSerializer::TMediumProperties& serializer, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

#define CHUNK_PROPERTIES_INL_H_
#include "chunk_properties-inl.h"
#undef  CHUNK_PROPERTIES_INL_H_
