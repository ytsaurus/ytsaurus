#pragma once

#include "public.h"
#include "chunk_requisition.h"
#include "chunk_replica.h"
#include "chunk_tree.h"
#include "incumbency_epoch.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/security_server/cluster_resources.h>

#include <yt/yt/server/lib/chunk_server/immutable_chunk_meta.h>

#include <yt/yt/ytlib/chunk_client/proto/chunk_info.pb.h>
#include <yt/yt/ytlib/chunk_client/proto/chunk_service.pb.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/library/erasure/public.h>

#include <yt/yt/core/misc/property.h>

#include <library/cpp/yt/containers/intrusive_linked_list.h>

#include <library/cpp/yt/small_containers/compact_vector.h>
#include <library/cpp/yt/small_containers/compact_flat_map.h>

#include <library/cpp/yt/memory/ref_tracked.h>

#include <optional>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TChunkExportData
{
    ui32 RefCounter = 0;
    TChunkRequisitionIndex ChunkRequisitionIndex = EmptyChunkRequisitionIndex;

    void Persist(const NCellMaster::TPersistenceContext& context);
};

static_assert(sizeof(TChunkExportData) == 8, "sizeof(TChunkExportData) != 8");

using TLegacyCellIndexToChunkExportData = TCompactFlatMap<int, TChunkExportData, TypicalChunkExportFactor>;
static_assert(sizeof(TLegacyCellIndexToChunkExportData::value_type) == 12,
    "sizeof(TLegacyCellIndexToChunkExportData::value_type) != 12");
static_assert(sizeof(TLegacyCellIndexToChunkExportData) == 56,
    "sizeof(TLegacyCellIndexToChunkExportData) != 56");

using TCellTagToChunkExportData = TCompactFlatMap<NObjectServer::TCellTag, TChunkExportData, TypicalChunkExportFactor>;
static_assert(sizeof(TCellTagToChunkExportData::value_type) == 12, "sizeof(TCellTagToChunkExportData::value_type) != 12");
static_assert(sizeof(TCellTagToChunkExportData) == 56, "sizeof(TCellTagToChunkExportData) != 56");

////////////////////////////////////////////////////////////////////////////////

struct TChunkDynamicData
    : public NObjectServer::TObjectDynamicData
{
    using TMediumToRepairQueueIterator = TCompactFlatMap<int, TChunkRepairQueueIterator, 2>;

    using TJobSet = TCompactVector<TJobPtr, 1>;

    //! The time since this chunk needs repairing.
    NProfiling::TCpuInstant EpochPartLossTime = {};

    //! Indicates that certain background scans were scheduled for this chunk.
    EChunkScanKind EpochScanFlags = EChunkScanKind::None;

    //! Indicates for which epoch #EpochScanFlags (besides refresh) is valid.
    NObjectServer::TEpoch Epoch = 0;

    //! Indicates for which epoch incumbency scan flags and #EpochPartLostTime are valid.
    TIncumbencyEpoch IncumbencyEpoch = NullIncumbencyEpoch;

    //! Indicates last incumbency epoch chunk was refreshed in.
    //! This is used to check whether chunk state in replicator is actual.
    TIncumbencyEpoch LastRefreshIncumbencyEpoch = NullIncumbencyEpoch;

    //! For each medium, contains a valid iterator for those chunks belonging to the repair queue
    //! and null (default iterator value) for others.
    TMediumToRepairQueueIterator MissingPartRepairQueueIterators;
    TMediumToRepairQueueIterator DecommissionedPartRepairQueueIterators;

    //! Set of jobs that are currently scheduled for this chunk.
    TJobSet Jobs;

    //! All blob chunks are linked via this node, as are all journal
    //! chunks. (The two lists are separate.)
    TIntrusiveLinkedListNode<TChunk> LinkedListNode;
};
static_assert(sizeof(TChunkDynamicData) == 144);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EChunkRepairQueue,
    ((Missing)           (0))
    ((Decommissioned)    (1))
);

////////////////////////////////////////////////////////////////////////////////

class TChunk
    : public TChunkTree
    , public TRefTracked<TChunk>
{
public:
    DEFINE_BYREF_RW_PROPERTY(TImmutableChunkMetaPtr, ChunkMeta);

    // This map is typically small, e.g. has the size of 1.
    using TParents = TCompactFlatMap<TChunkTree*, int, TypicalChunkParentCount>;
    DEFINE_BYREF_RO_PROPERTY(TParents, Parents);

    DEFINE_BYVAL_RW_PROPERTY(TConsistentReplicaPlacementHash, ConsistentReplicaPlacementHash, NullConsistentReplicaPlacementHash);

    DEFINE_BYVAL_RW_PROPERTY(NNodeTrackerServer::TNode*, NodeWithEndorsement);

    DEFINE_BYVAL_RW_PROPERTY(i64, DiskSpace);

    // NB: It's necessary to hold a strong ref on schema due to the fact that
    // a chunk can have a schema that's different from that of its table.
    // This can happen, for example, when chunk_sort_columns is used during upload.
    DEFINE_BYREF_RW_PROPERTY(NTableServer::TMasterTableSchemaPtr, Schema);

    //! Some TMiscExt fields extracted for effective access.
    DEFINE_BYVAL_RO_PROPERTY(i64, RowCount);
    DEFINE_BYVAL_RO_PROPERTY(i64, CompressedDataSize);
    DEFINE_BYVAL_RO_PROPERTY(i64, UncompressedDataSize);
    DEFINE_BYVAL_RO_PROPERTY(i64, DataWeight);
    DEFINE_BYVAL_RO_PROPERTY(i64, MaxBlockSize);
    DEFINE_BYVAL_RO_PROPERTY(NCompression::ECodec, CompressionCodec);
    DEFINE_BYVAL_RO_PROPERTY(i64, SystemBlockCount);

    DEFINE_BYVAL_RW_PROPERTY(NErasure::ECodec, ErasureCodec);

    //! Indicates that the list of replicas has changed and endorsement
    //! for ally replicas announcement should be registered.
    DEFINE_BYVAL_RW_PROPERTY(bool, EndorsementRequired);

    DEFINE_BYVAL_RW_PROPERTY(i8, ReadQuorum);
    DEFINE_BYVAL_RW_PROPERTY(i8, WriteQuorum);

    //! Cached |GetChunkShardIndex(id)| for efficient access.
    DEFINE_BYVAL_RO_PROPERTY(i8, ShardIndex);

public:
    explicit TChunk(TChunkId id);

    TChunkDynamicData* GetDynamicData() const;

    TChunkTreeStatistics GetStatistics() const;

    //! Get disk size of a single part of the chunk.
    /*!
     *  For a non-erasure chunk, simply returns its size
     *  (same as GetDiskSpace()).
     *  For an erasure chunk, returns that size divided by the number of parts
     *  used by the codec.
     */
    i64 GetPartDiskSpace() const;

    TString GetLowercaseObjectName() const override;
    TString GetCapitalizedObjectName() const override;
    TString GetObjectPath() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void AddParent(TChunkTree* parent);
    void RemoveParent(TChunkTree* parent);
    int GetParentCount() const;
    bool HasParents() const;

    TRange<TChunkLocationPtrWithReplicaInfo> StoredReplicas() const;

    //! For non-erasure chunks, contains a FIFO queue of seen replicas; its tail position is kept in #CurrentLastSeenReplicaIndex_.
    //! For erasure chunks, this array is directly addressed by replica indexes; at most one replica is kept per part.
    TRange<TNodeId> LastSeenReplicas() const;

    void AddReplica(
        TChunkLocationPtrWithReplicaInfo replica,
        const TDomesticMedium* medium,
        bool approved);
    void RemoveReplica(TChunkLocationPtrWithReplicaIndex replica, bool approved);

    void ApproveReplica(TChunkLocationPtrWithReplicaInfo replica);
    int GetApprovedReplicaCount() const;

    // COMPAT(ifsmirnov)
    void SetApprovedReplicaCount(int count);

    void Confirm(
        const NChunkClient::NProto::TChunkInfo& chunkInfo,
        const NChunkClient::NProto::TChunkMeta& chunkMeta);

    bool GetMovable() const;
    void SetMovable(bool value);

    bool GetOverlayed() const;
    void SetOverlayed(bool value);

    void SetRowCount(i64 rowCount);

    bool IsConfirmed() const;

    bool GetScanFlag(EChunkScanKind kind) const;
    void SetScanFlag(EChunkScanKind kind);
    void ClearScanFlag(EChunkScanKind kind);

    TChunk* GetNextScannedChunk() const;

    std::optional<NProfiling::TCpuInstant> GetPartLossTime() const;
    void SetPartLossTime(NProfiling::TCpuInstant partLossTime);
    void ResetPartLossTime();

    TChunkRepairQueueIterator GetRepairQueueIterator(int mediumIndex, EChunkRepairQueue queue) const;
    void SetRepairQueueIterator(int mediumIndex, EChunkRepairQueue queue, TChunkRepairQueueIterator value);
    TChunkDynamicData::TMediumToRepairQueueIterator* SelectRepairQueueIteratorMap(EChunkRepairQueue queue) const;

    const TChunkDynamicData::TJobSet& GetJobs() const;

    bool HasJobs() const;
    void AddJob(TJobPtr job);
    void RemoveJob(const TJobPtr& job);

    //! Refs all (local, external and aggregated) requisitions this chunk uses.
    //! Supposed to be called soon after the chunk is constructed or loaded.
    void RefUsedRequisitions(TChunkRequisitionRegistry* registry) const;

    //! A reciprocal to the above. Called at chunk destruction.
    void UnrefUsedRequisitions(
        TChunkRequisitionRegistry* registry,
        const NObjectServer::IObjectManagerPtr& objectManager) const;

    TChunkRequisitionIndex GetLocalRequisitionIndex() const;
    void SetLocalRequisitionIndex(
        TChunkRequisitionIndex requisitionIndex,
        TChunkRequisitionRegistry* registry,
        const NObjectServer::IObjectManagerPtr& objectManager);

    //! Prerequisite: IsExportedToCell(cellTag).
    TChunkRequisitionIndex GetExternalRequisitionIndex(NObjectServer::TCellTag cellTag) const;
    //! Prerequisite: IsExportedToCell(cellTag).
    void SetExternalRequisitionIndex(
        NObjectServer::TCellTag cellTag,
        TChunkRequisitionIndex requisitionIndex,
        TChunkRequisitionRegistry* registry,
        const NObjectServer::IObjectManagerPtr& objectManager);

    //! Returns chunk's requisition aggregated from local and external values.
    //! If aggregating them would result in an empty requisition, returns the most
    //! recent non-empty aggregated requisition.
    //! For semantics of aggregation, see #TChunkRequisition::operator|=().
    const TChunkRequisition& GetAggregatedRequisition(const TChunkRequisitionRegistry* registry) const;

    TChunkRequisitionIndex GetAggregatedRequisitionIndex() const;

    //! Returns chunk's replication aggregated from local and external values.
    //! For semantics of aggregation, see #TChunkReplication::operator|=().
    /*!
     *  NB: by default only COMMITTED OWNERS affect this. If the chunk has no
     *  committed owners, then non-committed ones are taken into account.
     *
     *  If there're no owners at all, the returned value is the most recent
     *  non-empty aggregated replication.
     */
    const TChunkReplication& GetAggregatedReplication(const TChunkRequisitionRegistry* registry) const;

    //! Returns the replication factor for the specified medium aggregated from
    //! the local and the external values. See #GetAggregatedReplication().
    int GetAggregatedReplicationFactor(int mediumIndex, const TChunkRequisitionRegistry* registry) const;

    //! Returns the number of physical replicas the chunk should be replicated to.
    //! Unlike similar methods, non-committed owners always contribute to this value.
    int GetAggregatedPhysicalReplicationFactor(const TChunkRequisitionRegistry* registry) const;

    //! Returns the number of physical replicas on particular medium. This equals to:
    //!   - RF for regular chunks,
    //!   - total part count for erasure chunks (or data part if dataPartsOnly is set).
    int GetPhysicalReplicationFactor(int mediumIndex, const TChunkRequisitionRegistry* registry) const;

    i64 GetReplicaLagLimit() const;
    void SetReplicaLagLimit(i64 value);

    std::optional<i64> GetFirstOverlayedRowIndex() const;
    void SetFirstOverlayedRowIndex(std::optional<i64> value);

    //! Returns |true| iff this is an erasure chunk.
    bool IsErasure() const;

    //! Returns |true| iff this is a journal chunk.
    bool IsJournal() const;

    //! Returns |true| iff this is a blob chunk.
    bool IsBlob() const;

    //! Returns |true| iff the chunk can be read immediately, i.e. without repair.
    /*!
     *  For regular (non-erasure) chunk this is equivalent to the existence of any replica.
     *  For erasure chunks this is equivalent to the existence of replicas for all data parts.
     */
    bool IsAvailable() const;

    //! Returns |true| iff this is a sealed journal chunk.
    //! For blob chunks always returns |true|.
    bool IsSealed() const;

    void SetSealed(bool value);

    bool GetStripedErasure() const;
    void SetStripedErasure(bool value);

    bool GetSealable() const;
    void SetSealable(bool value);

    //! Flag indicating that chunk had small replication factor at least once.
    //! Such chunks are never considered vital in chunk replicator even if
    //! current placement is safe.
    bool GetHistoricallyNonVital() const;
    void SetHistoricallyNonVital(bool value);

    i64 GetPhysicalSealedRowCount() const;

    //! Marks the chunk as sealed, i.e. sets its ultimate row count, data size etc.
    void Seal(const NChunkClient::NProto::TChunkSealInfo& info);

    //! For journal chunks, returns true iff the chunk is sealed.
    //! For blob chunks, return true iff the chunk is confirmed.
    bool IsDiskSizeFinal() const;

    //! Returns the maximum number of replicas that can be stored in the same
    //! failure domain without violating the availability guarantees.
    /*!
     *  As #GetAggregatedReplication(), takes into account only committed owners of
     *  this chunk, if there're any. Otherwise falls back to all owners.
     *
     *  \param replicationFactorOverride An override for replication factor;
     *  used when one wants to upload fewer replicas but still guarantee placement safety.
     */
    int GetMaxReplicasPerFailureDomain(
        int mediumIndex,
        std::optional<int> replicationFactorOverride,
        const TChunkRequisitionRegistry* registry) const;

    //! Returns the export data w.r.t. to a cell.
    TChunkExportData GetExportData(NObjectServer::TCellTag cellTag) const;

    //! Same as GetExportData(cellTag).RefCounter != 0.
    bool IsExportedToCell(NObjectServer::TCellTag cellTag) const;

    bool IsExported() const;

    //! Increments export ref counter.
    void Export(NObjectServer::TCellTag cellTag, TChunkRequisitionRegistry* registry);

    //! Decrements export ref counter.
    void Unexport(
        NObjectServer::TCellTag cellTag,
        int importRefCounter,
        TChunkRequisitionRegistry* registry,
        const NObjectServer::IObjectManagerPtr& objectManager);

    i64 GetMasterMemoryUsage() const;

    //! Extracts chunk type from meta.
    NChunkClient::EChunkType GetChunkType() const;

    //! Extracts chunk format from meta.
    NChunkClient::EChunkFormat GetChunkFormat() const;

    bool HasConsistentReplicaPlacementHash() const;

    //! Called when chunk is being refreshed.
    void OnRefresh();

    //! Returns true if chunk was refreshed in current refresh epoch
    //! and false otherwise.
    bool IsRefreshActual() const;

    // COMPAT(kvk1920)
    void TransformOldExportData(const NObjectServer::TCellTagList& registeredCellTags);

private:
    //! -1 stands for std::nullopt for non-overlayed chunks.
    i64 FirstOverlayedRowIndex_ = -1;

    // COMPAT(kvk1920)
    class TPerCellExportData
        : NNonCopyable::TNonCopyable
    {
    public:
        ~TPerCellExportData();

        void Load(NCellMaster::TLoadContext& context);
        void Save(NCellMaster::TSaveContext& context) const;

        explicit operator bool() const noexcept;

        TCellTagToChunkExportData& operator*() const noexcept;
        TCellTagToChunkExportData* operator->() const noexcept;
        TCellTagToChunkExportData* Get() const noexcept;

        void MaybeInit();
        void MaybeShrink();

        void TransformCellIndicesToCellTags(const NObjectServer::TCellTagList& registeredCellTags);

    private:
        // The lowest bit is used to distinguish cell indices and cell tags.
        static_assert(sizeof(uintptr_t) == 8, "sizeof(uintptr_t) != 8");
        static_assert(alignof(TLegacyCellIndexToChunkExportData) >= 2);
        static_assert(alignof(TCellTagToChunkExportData) >= 2);
        constexpr static uintptr_t LegacyExportDataMask = 1;

        uintptr_t Ptr_ = 0;

        TLegacyCellIndexToChunkExportData* LegacyGet() const noexcept;
    };

    //! Per-cell data.
    TPerCellExportData PerCellExportData_;

    TChunkRequisitionIndex AggregatedRequisitionIndex_;
    TChunkRequisitionIndex LocalRequisitionIndex_;

    //! Ceil(log_2 x), where x is an upper bound
    //! for the difference between length of any
    //! two replicas of a journal chunk.
    ui8 LogReplicaLagLimit_ = 0;

    struct
    {
        bool Movable : 1;
        bool Overlayed : 1;
        bool Sealed : 1;
        bool StripedErasure : 1;
        bool Sealable : 1;
        bool HistoricallyNonVital : 1;
    } Flags_ = {};

    struct TReplicasDataBase
    {
        //! Number of approved replicas among stored.
        int ApprovedReplicaCount = 0;

        //! Indicates the position in LastSeenReplicas to be written next.
        int CurrentLastSeenReplicaIndex = 0;

        virtual ~TReplicasDataBase() = default;

        virtual void Initialize() = 0;

        virtual TRange<TChunkLocationPtrWithReplicaInfo> GetStoredReplicas() const = 0;
        virtual TMutableRange<TChunkLocationPtrWithReplicaInfo> MutableStoredReplicas() = 0;
        virtual void AddStoredReplica(TChunkLocationPtrWithReplicaInfo replica) = 0;
        virtual void RemoveStoredReplica(int replicaIndex) = 0;

        //! Null entries are InvalidNodeId.
        virtual TRange<TNodeId> GetLastSeenReplicas() const = 0;
        virtual TMutableRange<TNodeId> MutableLastSeenReplicas() = 0;

        virtual void Load(NCellMaster::TLoadContext& context) = 0;
        virtual void Save(NCellMaster::TSaveContext& context) const = 0;
    };

    template <size_t TypicalStoredReplicaCount, size_t LastSeenReplicaCount>
    struct TReplicasData
        : public TReplicasDataBase
    {
        using TStoredReplicas = TCompactVector<TChunkLocationPtrWithReplicaInfo, TypicalStoredReplicaCount>;

        TStoredReplicas StoredReplicas;

        std::array<TNodeId, LastSeenReplicaCount> LastSeenReplicas;

        void Initialize() override;

        TRange<TNodeId> GetLastSeenReplicas() const override;
        TMutableRange<TNodeId> MutableLastSeenReplicas() override;

        //! Null entries are InvalidNodeId.
        TRange<TChunkLocationPtrWithReplicaInfo> GetStoredReplicas() const override;
        TMutableRange<TChunkLocationPtrWithReplicaInfo> MutableStoredReplicas() override;
        void AddStoredReplica(TChunkLocationPtrWithReplicaInfo replica) override;
        void RemoveStoredReplica(int replicaIndex) override;

        void Load(NCellMaster::TLoadContext& context) override;
        void Save(NCellMaster::TSaveContext& context) const override;
    };

    constexpr static int RegularChunkTypicalReplicaCount = 5;
    constexpr static int RegularChunkLastSeenReplicaCount = 5;
    using TRegularChunkReplicasData = TReplicasData<RegularChunkTypicalReplicaCount, RegularChunkLastSeenReplicaCount>;

    constexpr static int ErasureChunkTypicalReplicaCount = 24;
    constexpr static int ErasureChunkLastSeenReplicaCount = 16;
    static_assert(ErasureChunkLastSeenReplicaCount >= ::NErasure::MaxTotalPartCount, "ErasureChunkLastSeenReplicaCount < NErasure::MaxTotalPartCount");
    using TErasureChunkReplicasData = TReplicasData<ErasureChunkTypicalReplicaCount, ErasureChunkLastSeenReplicaCount>;

    // COMPAT(gritukan)
    constexpr static int OldLastSeenReplicaCount = 16;

    //! This additional indirection helps to save up some space since
    //! no replicas are being maintained for foreign chunks.
    //! It also separates relatively mutable data from static one,
    //! which helps to avoid excessive CoW during snapshot construction.
    std::unique_ptr<TReplicasDataBase> ReplicasData_;

    TChunkRequisition ComputeAggregatedRequisition(const TChunkRequisitionRegistry* registry);

    const TReplicasDataBase& ReplicasData() const;
    TReplicasDataBase* MutableReplicasData();

    void UpdateAggregatedRequisitionIndex(
        TChunkRequisitionRegistry* registry,
        const NObjectServer::IObjectManagerPtr& objectManager);

    void MaybeResetObsoleteEpochData();

    void OnMiscExtUpdated(const NChunkClient::NProto::TMiscExt& miscExt);

    using TEmptyChunkReplicasData = TReplicasData<0, 0>;
    static const TEmptyChunkReplicasData EmptyChunkReplicasData;
};

DEFINE_MASTER_OBJECT_TYPE(TChunk)

static_assert(sizeof(TChunk) == 296, "sizeof(TChunk) != 296");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

#define CHUNK_INL_H_
#include "chunk-inl.h"
#undef CHUNK_INL_H_
