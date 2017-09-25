#pragma once

#include <yt/server/cell_master/public.h>

#include <yt/server/chunk_server/public.h>
#include <yt/server/chunk_server/chunk_manager.pb.h>

#include <yt/server/security_server/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/core/misc/property.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/yson_serializable.h>

#include <array>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

//! Settings specifying how a chunk or a chunk owner should be replicated over a medium.
struct TReplicationPolicy
{
public:
    TReplicationPolicy();

    TReplicationPolicy(int replicationFactor, bool dataPartsOnly);

    void Clear();

    int GetReplicationFactor() const;
    void SetReplicationFactor(int replicationFactor);

    bool GetDataPartsOnly() const;
    void SetDataPartsOnly(bool dataPartsOnly);

    //! Returns true iff replication factor is not zero.
    /*!
     *  Semantically, this means that the subject to this policy on a particular medium
     *  has that medium 'enabled' (the subject being a chunk or a chunk owner).
     */
    explicit operator bool() const;

    //! Combines this object with #rhs by MAXing replication factors and ANDing
    //! 'data parts only' flags.
    TReplicationPolicy& operator|=(const TReplicationPolicy& rhs);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

protected:
    ui8 ReplicationFactor_ : 7;
    // For certain media, it may be helpful to be able to store just the data
    // parts of erasure coded chunks.
    bool DataPartsOnly_ : 1;
};

static_assert(sizeof(TReplicationPolicy) == 1, "sizeof(TReplicationPolicy) != 1");

bool operator==(TReplicationPolicy lhs, TReplicationPolicy rhs);
bool operator!=(TReplicationPolicy lhs, TReplicationPolicy rhs);

void FormatValue(TStringBuilder* builder, TReplicationPolicy policy, const TStringBuf& format);
TString ToString(TReplicationPolicy policy);

////////////////////////////////////////////////////////////////////////////////

//! Settings specifying how a chunk or a chunk owner should be replicated over each medium.
/*!
 *  This includes the 'vital' flag, because even though it doesn't affect
 *  replication, it's nevertheless related.
 *
 *  In contrast to TChunkRequisition, this class doesn't concert itself with
 *  accounts: it's only interested in *how much* this chunk should be
 *  replicated, not who ordered such replication.
 */
class TChunkReplication
{
public:
    using TMediumReplicationPolicyArray = TPerMediumArray<TReplicationPolicy>;

    using const_iterator = typename TMediumReplicationPolicyArray::const_iterator;
    using iterator = typename TMediumReplicationPolicyArray::iterator;

    //! Clear everything and prepare for combining (via operator|=()).
    /*!
     *  In particular, this sets 'data parts only' to true for all media.
     *
     *  NB: this method is necessary because THE STATE CONSTRUCTED BY THE
     *  DEFAULT CTOR IS UNSAFE! It has replication factors set to zero and thus
     *  doesn't represent a sensible set of defaults for a chunk owner. Neither
     *  it is suitable for combining with other sets via #operator|=(). In
     *  particular, 'data parts only' flags are combined by ANDing, and the
     *  default value (of |false|) would affect the end result of combining.
     */
    void ClearForCombining();

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    const_iterator begin() const;
    const_iterator end() const;
    const_iterator cbegin() const;
    const_iterator cend() const;
    iterator begin();
    iterator end();

    const TReplicationPolicy& operator[](int mediumIndex) const;
    TReplicationPolicy& operator[](int mediumIndex);

    bool GetVital() const;
    void SetVital(bool vital);

    //! Combines this with #rhs by ORing 'vital' flags and combining replication
    //! policies for each medium (see #TReplicationPolicy::operator|=()).
    TChunkReplication& operator|=(const TChunkReplication& rhs);

    //! Returns |true| iff this replication settings would not result in a data
    //! loss (i.e. on at least on medium, replication factor is non-zero and
    //! 'data parts only' flag is set to |false|).
    bool IsValid() const;

private:
    TMediumReplicationPolicyArray MediumReplicationPolicies = {};
    bool Vital_ = false;

    static_assert(
        sizeof(MediumReplicationPolicies) == MaxMediumCount * sizeof(TReplicationPolicy),
        "sizeof(MediumReplicationPolicies) != MaxMediumCount * sizeof(TReplicationPolicy)");
};

static_assert(sizeof(TChunkReplication) == 8, "TChunkReplication's size is wrong");

bool operator==(const TChunkReplication& lhs, const TChunkReplication& rhs);
bool operator!=(const TChunkReplication& lhs, const TChunkReplication& rhs);

void FormatValue(TStringBuilder* builder, const TChunkReplication& replication, const TStringBuf& format);
TString ToString(const TChunkReplication& replication);

////////////////////////////////////////////////////////////////////////////////

//! A helper class for YSON-serializing media-specific parts of #TChunkReplication
//! (viz. everything except the 'vital' flag).
/*!
 *  [De]serializing that class directly is not an option since that would
 *  require mapping medium indexes to their names, which can't be done without
 *  #TChunkManager.
 */
class TSerializableChunkReplication
{
public:
    TSerializableChunkReplication() = default;
    TSerializableChunkReplication(
        const TChunkReplication& replication,
        const TChunkManagerPtr& chunkManager);

    void ToChunkReplication(
        TChunkReplication* replication,
        const TChunkManagerPtr& chunkManager);

    void Serialize(NYson::IYsonConsumer* consumer) const;
    void Deserialize(NYTree::INodePtr node);

private:
    struct TReplicationPolicy
    {
        int ReplicationFactor = 0;
        bool DataPartsOnly = false;
    };

    //! Media are sorted by name at serialization. This is by no means required,
    //! we're just being nice here.
    std::map<TString, TReplicationPolicy> MediumReplicationPolicies_;

    friend void Serialize(const TReplicationPolicy& serializer, NYson::IYsonConsumer* consumer);
    friend void Deserialize(TReplicationPolicy& serializer, NYTree::INodePtr node);
};

void Serialize(const TSerializableChunkReplication& serializer, NYson::IYsonConsumer* consumer);
void Deserialize(TSerializableChunkReplication& serializer, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

void ValidateReplicationFactor(int replicationFactor);
void ValidateChunkReplication(
    const TChunkManagerPtr& chunkManager,
    const TChunkReplication& replication,
    int primaryMediumIndex);

////////////////////////////////////////////////////////////////////////////////

struct TRequisitionEntry
{
    NSecurityClient::TAccountId AccountId = NObjectClient::NullObjectId;
    int MediumIndex = NChunkClient::InvalidMediumIndex;
    TReplicationPolicy ReplicationPolicy;
    // The 'committed' flag is necessary in order to decide which quota usage to
    // charge (committed or uncommitted).
    //
    // NB: when accounting is involved, this flag is tricky: a combination of
    // two entries: "committed, RF == 5" and "not committed, RF == 3" (accounts
    // and media being equal) should be charged from the committed quota
    // only. On the other hand, a combination of "committed, RF == 3" and "not
    // committed, RF == 5" should be charged from the committed quota with a
    // factor of 3 and from the non-committed quota with a factor of 2. Thus, in
    // a sense, committed entries are charged first and non-committed - second.
    bool Committed = false;

    TRequisitionEntry(
        const NSecurityClient::TAccountId& accountId,
        int mediumIndex,
        TReplicationPolicy replicationPolicy,
        bool committed);

    // For persistence.
    TRequisitionEntry() = default;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    bool operator<(const TRequisitionEntry& rhs) const;
    bool operator==(const TRequisitionEntry& rhs) const;
    size_t Hash() const;
};

TString ToString(const TRequisitionEntry& entry);

////////////////////////////////////////////////////////////////////////////////

//! A summary of which accounts use a chunk, which media it's present on, and
//! how it should be replicated there. Includes 'vital' and 'committed' flags.
/*!
 *  In contrast to #TChunkReplication, this class provides detailed info on who
 *  ordered the replication of the chunk, how much replication and on which
 *  media.
 *
 *  A requisition is strictly larger than a replication: one can convert the
 *  former to the latter by stripping off all account info and, for each medium,
 *  combining replication policies (via #TReplicationPolicy::operator|=()).
 *  The reverse transformation is, of course, impossible (however, see #CombineWith()).
 */
class TChunkRequisition
{
public:
    // Actually, the entries vector is most likely to contain a single element.
    // However, additional space is needed when merging new items into it, and
    // branching a chunk owner node also introduces additional items.
    using TEntries = SmallVector<TRequisitionEntry, 4>;
    using const_iterator = TEntries::const_iterator;

    static TChunkRequisition FromProto(const NProto::TReqUpdateChunkRequisition::TChunkRequisition& protoRequsition);

    //! Constructs an 'empty' requisition with no entries.
    TChunkRequisition() = default;

    //! Constructs a requisition with a single entry.
    TChunkRequisition(
        const NSecurityClient::TAccountId& accountId,
        int mediumIndex,
        TReplicationPolicy replicationPolicy,
        bool committed);

    TChunkRequisition(const TChunkRequisition&) = default;
    TChunkRequisition(TChunkRequisition&&) = default;

    TChunkRequisition& operator=(const TChunkRequisition&) = default;
    TChunkRequisition& operator=(TChunkRequisition&&) = default;

    void ToProto(NProto::TReqUpdateChunkRequisition::TChunkRequisition* protoRequsition) const;
    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    const_iterator begin() const;
    const_iterator end() const;
    const_iterator cbegin() const;
    const_iterator cend() const;

    size_t EntryCount() const;

    bool GetVital() const;
    void SetVital(bool vital);

    void ForceReplicationFactor(int replicationFactor);

    //! Combines this with #rhs ORing vitalities and merging entries.
    TChunkRequisition& operator|=(const TChunkRequisition& rhs);

    void CombineWith(const TChunkReplication& replication, const NSecurityServer::TAccountId accountId, bool committed);

    //! Convert this requisition to a replication.
    /*!
     *  By default, ONLY COMMITTED ENTRIES are taken into account. If, however
     *  there aren't any, non-committed ones are used.
     *
     *  For those media that aren't mentioned by the requisition, the resulting
     *  replication will have RF set to zero. In particular, an empty
     *  requisition produces a replication with RF == 0 for all media. For media
     *  with RF == 0, the state of the 'data parts only' flag IS NOT SPECIFIED.
     *
     */
    TChunkReplication ToReplication() const;

    bool operator==(const TChunkRequisition& rhs) const;

    size_t Hash() const;

private:
    void CombineEntries(const TEntries& newEntries);

    void NormalizeEntries();

    // Maintained sorted.
    TEntries Entries_;
    // The default value matters! Because operator|=() ORs vitalities, empty
    // chunk requisition must start with false.
    bool Vital_ = false;
};

TString ToString(const TChunkRequisition& requisition);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

template <>
struct hash<NYT::NChunkServer::TRequisitionEntry>
{
    size_t operator()(const NYT::NChunkServer::TRequisitionEntry& entry) const
    {
        return entry.Hash();
    }
};

template <>
struct hash<NYT::NChunkServer::TChunkRequisition>
{
    size_t operator()(const NYT::NChunkServer::TChunkRequisition& requisition) const
    {
        return requisition.Hash();
    }
};

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkRequisitionRegistry
{
public:
    TChunkRequisitionRegistry(const NSecurityServer::TSecurityManagerPtr& securityManager);
    // For persistence only.
    TChunkRequisitionRegistry() = default;

    TChunkRequisitionRegistry(const TChunkRequisitionRegistry&) = delete;
    TChunkRequisitionRegistry& operator=(const TChunkRequisitionRegistry&) = delete;

    // NB: ref counts are not persisted.
    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    const TChunkRequisition& GetRequisition(ui32 index) const;
    const TChunkReplication& GetReplication(ui32 index) const;

    //! Returns specified requisition's index. Allocates a new index if necessary.
    /*!
     *  Newly allocated indexes are not automatically Ref()ed and should be
     *  either Ref()ed manually.
     */
    ui32 GetIndex(const TChunkRequisition& requisition);

    // NB: even though items are refcounted, items with zero RC may be
    // intermittently present in the registry.
    void Ref(ui32 index);
    void Unref(ui32 index);

private:
    ui32 Insert(const TChunkRequisition& requisition);

    ui32 NextIndex();

private:
    struct TIndexedItem
    {
        TChunkRequisition Requisition;
        TChunkReplication Replication;
        i64 RefCount = 0;

        void Save(NCellMaster::TSaveContext& context) const;
        void Load(NCellMaster::TLoadContext& context);
    };

    yhash<ui32, TIndexedItem> Index_;
    yhash<TChunkRequisition, ui32> ReverseIndex_;

    ui32 NextIndex_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

#define CHUNK_REQUISITION_INL_H_
#include "chunk_requisition-inl.h"
#undef  CHUNK_REQUISITION_INL_H_
