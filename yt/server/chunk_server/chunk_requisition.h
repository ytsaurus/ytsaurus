#pragma once

#include <yt/server/cell_master/public.h>

#include <yt/server/chunk_server/public.h>
#include <yt/server/chunk_server/chunk_manager.pb.h>

#include <yt/server/security_server/public.h>
#include <yt/server/security_server/account.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/client/object_client/helpers.h>

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

    //! Aggregates this object with #rhs by MAXing replication factors and ANDing
    //! 'data parts only' flags.
    TReplicationPolicy& operator|=(const TReplicationPolicy& rhs);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

protected:
    ui8 ReplicationFactor_ : 7;
    //! For certain media, it may be helpful to be able to store just the data
    //! parts of erasure coded chunks.
    bool DataPartsOnly_ : 1;
};

static_assert(sizeof(TReplicationPolicy) == 1, "sizeof(TReplicationPolicy) != 1");

bool operator==(TReplicationPolicy lhs, TReplicationPolicy rhs);
bool operator!=(TReplicationPolicy lhs, TReplicationPolicy rhs);

void FormatValue(TStringBuilder* builder, TReplicationPolicy policy, TStringBuf /*spec*/);
TString ToString(TReplicationPolicy policy);

void Serialize(const TReplicationPolicy& policy, NYson::IYsonConsumer* consumer);
void Deserialize(TReplicationPolicy& policy, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

//! Settings specifying how a chunk or a chunk owner should be replicated over each medium.
/*!
 *  This includes the 'vital' flag, because even though it doesn't affect
 *  replication, it's nevertheless related.
 *
 *  In contrast to #TChunkRequisition, this class doesn't concern itself with
 *  accounts: it's only interested in *how much* this chunk should be
 *  replicated, not who ordered such replication.
 */
class TChunkReplication
{
public:
    using TMediumReplicationPolicyArray = TPerMediumArray<TReplicationPolicy>;

    using const_iterator = typename TMediumReplicationPolicyArray::const_iterator;
    using iterator = typename TMediumReplicationPolicyArray::iterator;

    //! Constructs an 'empty' replication.
    /*!
     *  THE STATE CONSTRUCTED BY THE DEFAULT CTOR IS UNSAFE!
     *  It has replication factors set to zero and thus doesn't represent a
     *  sensible set of defaults for a chunk owner.
     *
     *  By default, 'data parts only' flags are set to |false| for all media.
     *
     *  If #clearForAggregating is |true|, these flags are set to |true|. Since these
     *  flags are aggregated by ANDing, this makes the replication suitable for
     *  aggregating via operator|=().
     */
    TChunkReplication(bool clearForAggregating = false);

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

    //! Aggregates this with #rhs by ORing 'vital' flags and aggregating replication
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

void FormatValue(TStringBuilder* builder, TChunkReplication replication, TStringBuf /*spec*/);
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

//! If primaryMediumIndex is null, eschews primary medium-related validation.
void ValidateChunkReplication(
    const TChunkManagerPtr& chunkManager,
    const TChunkReplication& replication,
    TNullable<int> primaryMediumIndex);

////////////////////////////////////////////////////////////////////////////////

struct TRequisitionEntry
{
    // NB: the requisition registry only weak-refs accounts. This means that
    // #IsObjectAlive() checks are a must. Entries with dead accounts may be
    // safely ignored as accounting resources for deleted accounts is pointless.
    NSecurityServer::TAccount* Account = nullptr;
    int MediumIndex = NChunkClient::InvalidMediumIndex;
    TReplicationPolicy ReplicationPolicy;
    // The 'committed' flag is necessary in order to decide which quota usage to
    // charge to (committed or uncommitted).
    //
    // NB: when accounting is involved, this flag is tricky: a combination of
    // two entries: "committed, RF == 5" and "not committed, RF == 3" (accounts
    // and media being equal) should be charged to the committed quota only. On
    // the other hand, a combination of "committed, RF == 3" and "not committed,
    // RF == 5" should be charged to the committed quota with a factor of 3 and
    // to the non-committed quota with a factor of 2. Thus, in a sense,
    // committed entries are charged first and non-committed - second.
    bool Committed = false;

    TRequisitionEntry(
        NSecurityServer::TAccount* account,
        int mediumIndex,
        TReplicationPolicy replicationPolicy,
        bool committed);

    // For persistence.
    TRequisitionEntry() = default;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    bool operator<(const TRequisitionEntry& rhs) const;
    bool operator==(const TRequisitionEntry& rhs) const;
    size_t GetHash() const;
};

void FormatValue(TStringBuilder* builder, const TRequisitionEntry& entry, TStringBuf /*spec*/ = {});
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
 *  aggregating replication policies (via #TReplicationPolicy::operator|=()).
 *  The reverse transformation is, of course, impossible (however, see #AggregateWith()).
 */
class TChunkRequisition
{
public:
    // Actually, the entries vector is most likely to contain a single element.
    // However, additional space is needed when merging new items into it, and
    // branching a chunk owner node also introduces additional items.
    using TEntries = SmallVector<TRequisitionEntry, 4>;
    using const_iterator = TEntries::const_iterator;

    //! Constructs an 'empty' requisition with no entries.
    TChunkRequisition() = default;

    //! Constructs a requisition with a single entry.
    TChunkRequisition(
        NSecurityServer::TAccount* account,
        int mediumIndex,
        TReplicationPolicy replicationPolicy,
        bool committed);

    TChunkRequisition(const TChunkRequisition&) = default;
    TChunkRequisition(TChunkRequisition&&) = default;

    TChunkRequisition& operator=(const TChunkRequisition&) = default;
    TChunkRequisition& operator=(TChunkRequisition&&) = default;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    const_iterator begin() const;
    const_iterator end() const;
    const_iterator cbegin() const;
    const_iterator cend() const;

    size_t GetEntryCount() const;

    bool GetVital() const;
    void SetVital(bool vital);

    void ForceReplicationFactor(int replicationFactor);

    //! Aggregates this with #rhs ORing vitalities and merging entries.
    TChunkRequisition& operator|=(const TChunkRequisition& rhs);

    void AggregateWith(
        const TChunkReplication& replication,
        NSecurityServer::TAccount* account,
        bool committed);

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

    size_t GetHash() const;

private:
    // Maintained sorted.
    TEntries Entries_;
    // The default value matters! Because operator|=() ORs vitalities, empty
    // chunk requisition must start with false.
    bool Vital_ = false;

    void AggregateEntries(const TEntries& newEntries);

    void NormalizeEntries();

    // NB: does not normalize entries.
    void AddEntry(
        NSecurityServer::TAccount* account,
        int mediumIndex,
        TReplicationPolicy replicationPolicy,
        bool committed);

    friend void FromProto(
        TChunkRequisition* requisition,
        const NProto::TReqUpdateChunkRequisition::TChunkRequisition& protoRequsition,
        const NSecurityServer::TSecurityManagerPtr& securityManager);
};

void ToProto(
    NProto::TReqUpdateChunkRequisition::TChunkRequisition* protoRequisition,
    const TChunkRequisition& requisition);

void FormatValue(TStringBuilder* builder, const TChunkRequisition& requisition, TStringBuf /*spec*/ = {});
TString ToString(const TChunkRequisition& requisition);

////////////////////////////////////////////////////////////////////////////////

class TSerializableChunkRequisition
{
public:
    TSerializableChunkRequisition() = default;
    TSerializableChunkRequisition(const TChunkRequisition& requisition, const TChunkManagerPtr& chunkManager);

    void Serialize(NYson::IYsonConsumer* consumer) const;
    void Deserialize(NYTree::INodePtr node);

private:
    struct TEntry
    {
        TString Account;
        TString Medium;
        TReplicationPolicy ReplicationPolicy;
        bool Committed;
    };

    std::vector<TEntry> Entries_;

    friend void Serialize(const TEntry& entry, NYson::IYsonConsumer* consumer);
    friend void Deserialize(TEntry& entry, NYTree::INodePtr node);
};

void Serialize(const TSerializableChunkRequisition& serializer, NYson::IYsonConsumer* consumer);
void Deserialize(TSerializableChunkRequisition& serializer, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

template <>
struct THash<NYT::NChunkServer::TRequisitionEntry>
{
    size_t operator()(const NYT::NChunkServer::TRequisitionEntry& entry) const
    {
        return entry.GetHash();
    }
};

template <>
struct THash<NYT::NChunkServer::TChunkRequisition>
{
    size_t operator()(const NYT::NChunkServer::TChunkRequisition& requisition) const
    {
        return requisition.GetHash();
    }
};

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkRequisitionRegistry
{
public:
    //! Constructs an empty registry. It lacks even chunk-wise migration related
    //! requisitions, so #EnsureBuiltinRequisitionsInitialized() must be called
    //! subsequently.
    TChunkRequisitionRegistry() = default;

    TChunkRequisitionRegistry(const TChunkRequisitionRegistry&) = delete;
    TChunkRequisitionRegistry& operator=(const TChunkRequisitionRegistry&) = delete;

    // Makes the registry empty. #EnsureBuiltinRequisitionsInitialized() must be
    // called subsequently.
    void Clear();

    void EnsureBuiltinRequisitionsInitialized(
        NSecurityServer::TAccount* chunkWiseAccountingMigrationAccount,
        const NObjectServer::TObjectManagerPtr& objectManager);

    // NB: ref counts are not persisted.
    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    const TChunkRequisition& GetRequisition(TChunkRequisitionIndex index) const;
    const TChunkReplication& GetReplication(TChunkRequisitionIndex index) const;

    //! Returns specified requisition's index. Allocates a new index if necessary.
    /*!
     *  Newly allocated indexes are not automatically Ref()ed and should be
     *  Ref()ed manually.
     */
    TChunkRequisitionIndex GetOrCreate(
        const TChunkRequisition& requisition,
        const NObjectServer::TObjectManagerPtr& objectManager);

    //! Returns specified requisition's index or Null if no such requisition is registered.
    TNullable<TChunkRequisitionIndex> Find(const TChunkRequisition& requisition) const;

    // NB: even though items are refcounted, items with zero RC may be
    // intermittently present in the registry.
    void Ref(TChunkRequisitionIndex index);
    void Unref(
        TChunkRequisitionIndex index,
        const NObjectServer::TObjectManagerPtr& objectManager);

    void Serialize(NYson::IYsonConsumer* consumer, const TChunkManagerPtr& chunkManager) const;

private:
    struct TIndexedItem
    {
        TChunkRequisition Requisition;
        TChunkReplication Replication;
        i64 RefCount = 0;

        void Save(NCellMaster::TSaveContext& context) const;
        void Load(NCellMaster::TLoadContext& context);
    };

    THashMap<TChunkRequisitionIndex, TIndexedItem> IndexToItem_;
    THashMap<TChunkRequisition, TChunkRequisitionIndex> RequisitionToIndex_;

    TChunkRequisitionIndex NextIndex_ = EmptyChunkRequisitionIndex;

    TChunkRequisitionIndex GenerateIndex();

    TChunkRequisitionIndex Insert(
        const TChunkRequisition& requisition,
        const NObjectServer::TObjectManagerPtr& objectManager);

    void Erase(
        TChunkRequisitionIndex index,
        const NObjectServer::TObjectManagerPtr& objectManager);

    void FakeRefBuiltinRequisitions();
};

////////////////////////////////////////////////////////////////////////////////

// NB: used only for Orchid and thus doesn't support deserialization.
class TSerializableChunkRequisitionRegistry
{
public:
    explicit TSerializableChunkRequisitionRegistry(const TChunkManagerPtr& chunkManager);

    void Serialize(NYson::IYsonConsumer* consumer) const;

private:
    const TChunkManagerPtr ChunkManager_;
};

void Serialize(const TSerializableChunkRequisitionRegistry& serializer, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

// A temporary requisition registry that just manages the index-to-requisition
// mapping and does not deal with refcounting or persistence.
class TEphemeralRequisitionRegistry
{
public:
    const TChunkRequisition& GetRequisition(TChunkRequisitionIndex index) const;
    TChunkRequisitionIndex GetOrCreateIndex(const TChunkRequisition& requisition);

    void Clear();

private:
    TChunkRequisitionIndex Insert(const TChunkRequisition& requisition);
    TChunkRequisitionIndex GenerateIndex();

    THashMap<TChunkRequisitionIndex, TChunkRequisition> IndexToRequisition_;
    THashMap<TChunkRequisition, TChunkRequisitionIndex> RequisitionToIndex_;
    TChunkRequisitionIndex NextIndex_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void FillChunkRequisitionDict(NProto::TReqUpdateChunkRequisition* request, const T& requisitionRegistry);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

#define CHUNK_REQUISITION_INL_H_
#include "chunk_requisition-inl.h"
#undef  CHUNK_REQUISITION_INL_H_
