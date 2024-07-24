#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/chunk_server/proto/chunk_manager.pb.h>

#include <yt/yt/server/master/security_server/public.h>
#include <yt/yt/server/master/security_server/account.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/yson/public.h>

#include <array>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

//! Settings specifying how a chunk or a chunk owner should be replicated over a medium.
struct TReplicationPolicy
{
public:
    constexpr TReplicationPolicy()
        : ReplicationFactor_(0)
        , DataPartsOnly_(false)
    { }

    constexpr TReplicationPolicy(int replicationFactor, bool dataPartsOnly)
        : ReplicationFactor_(replicationFactor)
        , DataPartsOnly_(dataPartsOnly)
    { }

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
    TReplicationPolicy& operator|=(TReplicationPolicy rhs);

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

protected:
    ui8 ReplicationFactor_ : 7;
    //! For certain media, it may be helpful to be able to store just the data
    //! parts of erasure coded chunks.
    bool DataPartsOnly_ : 1;
};

static_assert(sizeof(TReplicationPolicy) == 1, "sizeof(TReplicationPolicy) != 1");

bool operator==(TReplicationPolicy lhs, TReplicationPolicy rhs);

void FormatValue(TStringBuilderBase* builder, TReplicationPolicy policy, TStringBuf /*spec*/);

void Serialize(TReplicationPolicy policy, NYson::IYsonConsumer* consumer);
void Deserialize(TReplicationPolicy& policy, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

//! Statistics that, given the erasure codec of a chunk, provide the total number
//! of physical replicas that the chunk is replicated to.
struct TPhysicalReplication
{
    // Sum of RFs over all (non-cache) media. Used for regular chunks.
    int ReplicaCount = 0;

    // Number of (non-cache) media. Used for erasure chunks.
    // NB: DataPartsOnly is ignored, so |MediumCount| * |codec.TotalPartCount|
    // may be larger than actual number of physically stored parts.
    int MediumCount = 0;
};

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
private:
    struct TEntryComparator;

public:
    class TEntry
    {
        // Entries are stored sorted by medium index, changing the index would
        // break internal invariants.
        DEFINE_BYVAL_RO_PROPERTY(ui8, MediumIndex);
        static_assert(MaxMediumCount <= std::numeric_limits<decltype(MediumIndex_)>::max());

        DEFINE_BYREF_RW_PROPERTY(TReplicationPolicy, Policy);

    public:
        TEntry() = default;
        TEntry(int mediumIndex, TReplicationPolicy policy);

        void Save(NCellMaster::TSaveContext& context) const;
        void Load(NCellMaster::TLoadContext& context);

        void Save(NCypressServer::TBeginCopyContext& context) const;
        void Load(NCypressServer::TEndCopyContext& context);

        bool operator==(const TEntry& rhs) const;

    private:
        friend struct TEntryComparator;
    };

private:
    struct TEntryComparator
    {
        bool operator()(const TEntry& lhs, const TEntry& rhs) const;
    };

    // Leave some space for the replicator to manipulate replication policies.
    static constexpr unsigned TypicalChunkMediumCount = 7;
    using TEntries = TCompactVector<TEntry, TypicalChunkMediumCount>;

public:
    using iterator = TEntries::iterator;
    using const_iterator = TEntries::const_iterator;

    //! Constructs an 'empty' replication.
    /*!
     *  THE STATE CONSTRUCTED BY THE DEFAULT CTOR IS UNSAFE!
     *  It has no entries and is thus effectively prescribes data removal.
     *  Obviously, this doesn't represent a sensible set of defaults for a chunk owner.
     */
    TChunkReplication() = default;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void Save(NCypressServer::TBeginCopyContext& context) const;
    void Load(NCypressServer::TEndCopyContext& context);

    iterator begin();
    iterator end();

    const_iterator begin() const;
    const_iterator end() const;
    const_iterator cbegin() const;
    const_iterator cend() const;

    //! Removes entries for all media.
    //! Does not affect the 'vital' flag.
    void ClearEntries();

    //! Returns true iff this replication has an entry for the specified medium.
    bool Contains(int mediumIndex) const;

    //! Removes the entry for the specified medium.
    /*!
     *  Since a missing policy is considered equivalent to an empty policy,
     *  erasing an entry for a medium essentially prescribes removing data from
     *  it (or not storing it in the first place).
     */
    bool Erase(int mediumIndex);

    //! Sets or updates replication policy for a specific medium.
    /*!
     *  If eraseEmpty is true, setting a policy with zero RF is equivalent to
     *  erasing the entry for the medium altogether. Otherwise, the empty policy
     *  will actually be stored within this replication.
     *
     */
    void Set(int mediumIndex, TReplicationPolicy policy, bool eraseEmpty = true);

    //! Updates the entry for the specified medium by aggregating it with the
    //! specified policy via #TReplicationPolicy::operator|=().
    /*!
     *  Unlike for Set, #policy must have positive RF.
     *
     *  If there's no entry for the medium, this is equivalent to calling Set().
     */
    void Aggregate(int mediumIndex, TReplicationPolicy policy);

    //! Looks up the entry for the specified medium.
    /*!
     *  If no entry exists, returns an empty policy (with 0 RF and true 'data
     *  parts only') but that policy is not added to this replication.
     */
    TReplicationPolicy Get(int mediumIndex) const;

    bool GetVital() const;
    void SetVital(bool vital);

    //! The difference between the two methods below is in whether or not the `Vital` flag itself is considered.
    bool IsDurable(const IChunkManagerPtr& chunkManager, bool isErasureChunk) const;
    //! This method always returns `false` for non-vital chunks.
    bool IsDurabilityRequired(const IChunkManagerPtr& chunkManager, bool isErasureChunk) const;

    //! Returns |true| iff this replication settings would not result in a data
    //! loss (i.e. on at least on medium, replication factor is non-zero and
    //! 'data parts only' flag is set to |false|).
    bool IsValid() const;

    //! Returns the number of entries in replication.
    int GetSize() const;

private:
    static constexpr const auto EmptyReplicationPolicy = TReplicationPolicy(0, true);

    TEntries Entries_;
    bool Vital_ = false;

    std::pair<iterator, bool> Insert(int mediumIndex, TReplicationPolicy policy);

    //! Looks up an entry for the specified medium.
    const_iterator Find(int mediumIndex) const;
    iterator Find(int mediumIndex);

    // Implementation detail of the above. Works for both const and non-const entries.
    template <class T>
    static auto Find(T& entries, int mediumIndex) -> decltype(entries.begin());
};

static_assert(sizeof(TChunkReplication) == 24, "TChunkReplication's size is wrong");

bool operator==(const TChunkReplication& lhs, const TChunkReplication& rhs);

void FormatValue(TStringBuilderBase* builder, const TChunkReplication& replication, TStringBuf /*spec*/);

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
        const IChunkManagerPtr& chunkManager);

    void ToChunkReplication(
        TChunkReplication* replication,
        const IChunkManagerPtr& chunkManager) const;

    void Serialize(NYson::IYsonConsumer* consumer) const;
    void Deserialize(NYTree::INodePtr node);

private:
    //! Media are sorted by name at serialization. This is by no means required,
    //! we're just being nice here.
    std::map<TString, TReplicationPolicy> Entries_;

    friend void Serialize(TReplicationPolicy serializer, NYson::IYsonConsumer* consumer);
    friend void Deserialize(TReplicationPolicy& serializer, NYTree::INodePtr node);
};

void Serialize(const TSerializableChunkReplication& serializer, NYson::IYsonConsumer* consumer);
void Deserialize(TSerializableChunkReplication& serializer, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

//! If primaryMediumIndex is null, eschews primary medium-related validation.
void ValidateChunkReplication(
    const IChunkManagerPtr& chunkManager,
    const TChunkReplication& replication,
    std::optional<int> primaryMediumIndex);

////////////////////////////////////////////////////////////////////////////////

struct TRequisitionEntry
{
    // NB: the requisition registry only weak-refs accounts. This means that
    // #IsObjectAlive() checks are a must. Entries with dead accounts may be
    // safely ignored for accounting purposes but not for the purposes of
    // hashing and comparisons.
    NSecurityServer::TAccount* Account = nullptr;
    int MediumIndex = NChunkClient::GenericMediumIndex;
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

void FormatValue(TStringBuilderBase* builder, const TRequisitionEntry& entry, TStringBuf /*spec*/ = {});

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
    using TEntries = TCompactVector<TRequisitionEntry, 4>;
    using const_iterator = TEntries::const_iterator;

    //! Constructs an empty requisition with no entries.
    TChunkRequisition() = default;

    //! Constructs a requisition with a single entry. RF within replicationPolicy must be positive.
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

    //! Sets the specified RF to all the entries. RF must be positive.
    void ForceReplicationFactor(int replicationFactor);

    //! Aggregates this with #rhs ORing vitalities and merging entries.
    TChunkRequisition& operator|=(const TChunkRequisition& rhs);

    void AggregateWith(
        const TChunkReplication& replication,
        NSecurityServer::TAccount* account,
        bool committed);

    //! Converts this requisition to a replication.
    /*!
     *  By default, ONLY COMMITTED ENTRIES are taken into account. If, however
     *  there aren't any, non-committed ones are used.
     */
    TChunkReplication ToReplication() const;

    //! Converts this requisition to physical replication.
    //! Unlike #ToReplication(), non-committed entries are included as well.
    TPhysicalReplication ToPhysicalReplication() const;

    bool operator==(const TChunkRequisition& rhs) const;

    size_t GetHash() const;

    int GetSize() const;

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
        const NSecurityServer::ISecurityManagerPtr& securityManager);
};

void ToProto(
    NProto::TReqUpdateChunkRequisition::TChunkRequisition* protoRequisition,
    const TChunkRequisition& requisition);

void FormatValue(TStringBuilderBase* builder, const TChunkRequisition& requisition, TStringBuf /*spec*/ = TStringBuf{"v"});

////////////////////////////////////////////////////////////////////////////////

class TSerializableChunkRequisition
{
public:
    TSerializableChunkRequisition() = default;
    TSerializableChunkRequisition(const TChunkRequisition& requisition, const IChunkManagerPtr& chunkManager);

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

} // namespace NYT::NChunkServer

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

namespace NYT::NChunkServer {

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
        const NObjectServer::IObjectManagerPtr& objectManager);

    // NB: ref counts are not persisted.
    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    const TChunkRequisition& GetRequisition(TChunkRequisitionIndex index) const;
    const TChunkReplication& GetReplication(TChunkRequisitionIndex index) const;
    const TPhysicalReplication& GetPhysicalReplication(TChunkRequisitionIndex index) const;

    //! Returns specified requisition's index. Allocates a new index if necessary.
    /*!
     *  Newly allocated indexes are not automatically Ref()ed and should be
     *  Ref()ed manually.
     */
    TChunkRequisitionIndex GetOrCreate(
        const TChunkRequisition& requisition,
        const NObjectServer::IObjectManagerPtr& objectManager);

    //! Returns specified requisition's index or null if no such requisition is registered.
    std::optional<TChunkRequisitionIndex> Find(const TChunkRequisition& requisition) const;

    // NB: even though items are refcounted, items with zero RC may be
    // intermittently present in the registry.
    void Ref(TChunkRequisitionIndex index);
    void Unref(
        TChunkRequisitionIndex index,
        const NObjectServer::IObjectManagerPtr& objectManager);

    void Serialize(NYson::IYsonConsumer* consumer, const IChunkManagerPtr& chunkManager) const;

private:
    struct TIndexedItem
    {
        TChunkRequisition Requisition;
        TChunkReplication Replication;
        TPhysicalReplication PhysicalReplication;
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
        const NObjectServer::IObjectManagerPtr& objectManager);

    void Erase(
        TChunkRequisitionIndex index,
        const NObjectServer::IObjectManagerPtr& objectManager);

    void FakeRefBuiltinRequisitions();
};

////////////////////////////////////////////////////////////////////////////////

// NB: used only for Orchid and thus doesn't support deserialization.
class TSerializableChunkRequisitionRegistry
{
public:
    explicit TSerializableChunkRequisitionRegistry(const IChunkManagerPtr& chunkManager);

    void Serialize(NYson::IYsonConsumer* consumer) const;

private:
    const IChunkManagerPtr ChunkManager_;
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

} // namespace NYT::NChunkServer

#define CHUNK_REQUISITION_INL_H_
#include "chunk_requisition-inl.h"
#undef  CHUNK_REQUISITION_INL_H_
