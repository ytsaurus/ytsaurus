#pragma once

#include "public.h"

#include <yt/yt/library/min_hash_digest/public.h>

namespace NYT::NLsm {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM_WITH_UNDERLYING_TYPE(EStoreCompactionHintKind, ui8,
    ((None)               (0))
    ((ChunkViewTooNarrow) (1))
    ((VersionedRowDigest) (2))
    ((MinHashDigest)      (3))
);

DEFINE_ENUM_WITH_UNDERLYING_TYPE(EPartitionCompactionHintKind, ui8,
    ((None)               (0))
    ((MinHashDigest)      (3))
);

EStoreCompactionHintKind CheckedToStoreCompactionHintKind(EPartitionCompactionHintKind kind);

static constexpr std::array StoreCompactionHintKinds = {
    std::pair(EStoreCompactionHintKind::ChunkViewTooNarrow, EPartitionCompactionHintKind::None),
    std::pair(EStoreCompactionHintKind::VersionedRowDigest, EPartitionCompactionHintKind::None),
    std::pair(EStoreCompactionHintKind::MinHashDigest, EPartitionCompactionHintKind::MinHashDigest),
};

static constexpr std::array CalculatableStoreCompactionHintKinds = {
    std::pair(EStoreCompactionHintKind::ChunkViewTooNarrow, EPartitionCompactionHintKind::None),
    std::pair(EStoreCompactionHintKind::VersionedRowDigest, EPartitionCompactionHintKind::None),
};

static constexpr std::array PartitionCompactionHintKinds = {
    std::pair(EStoreCompactionHintKind::MinHashDigest, EPartitionCompactionHintKind::MinHashDigest),
};

template <class T>
using TStoreCompactionHintArray = TEnumIndexedArray<
    EStoreCompactionHintKind,
    T,
    EStoreCompactionHintKind::ChunkViewTooNarrow,
    EStoreCompactionHintKind::MinHashDigest>;

template <class T>
using TCalculatableStoreCompactionHintArray = TEnumIndexedArray<
    EStoreCompactionHintKind,
    T,
    EStoreCompactionHintKind::ChunkViewTooNarrow,
    EStoreCompactionHintKind::VersionedRowDigest
>;

template <class T>
using TPartitionCompactionHintArray = TEnumIndexedArray<
    EPartitionCompactionHintKind,
    T,
    EPartitionCompactionHintKind::MinHashDigest,
    EPartitionCompactionHintKind::MinHashDigest
>;

////////////////////////////////////////////////////////////////////////////////

class TCompactionHintBase
{
public:
    // Tablet node stuff.

    // Revision of store/partition (mutation sequence number) which prevents
    // tablet node from applying outdated feedback from LSM.
    DEFINE_BYVAL_RW_PROPERTY(NHydra::TRevision, NodeObjectRevision);

    // LSM stuff.

    // Revision of object(store/partition), when decision for upcoming compaction was made.
    DEFINE_BYVAL_RO_PROPERTY(NHydra::TRevision, LsmDecisionRevision);

    DEFINE_BYVAL_RO_PROPERTY(TInstant, Timestamp);
    DEFINE_BYVAL_RW_PROPERTY(EStoreCompactionReason, Reason, EStoreCompactionReason::None);

    // For store compaction hint defines its kind.
    // For partition compaction hint define kind of store payload.
    // If None - whole hint is interpreted as null.
    DEFINE_BYVAL_RO_PROPERTY(EStoreCompactionHintKind, StoreCompactionHintKind);

    // For store compaction hint define kind of payload dependent partition hint or none if there is no
    // such partition compaction hint.
    // For partition compaction hint defines its kind.
    DEFINE_BYVAL_RO_PROPERTY(EPartitionCompactionHintKind, PartitionCompactionHintKind);

public:
    TCompactionHintBase(
        EStoreCompactionHintKind storeCompactionHintKind = EStoreCompactionHintKind::None,
        EPartitionCompactionHintKind partitionCompactionHintKind = EPartitionCompactionHintKind::None);

    operator bool() const;

    // Lsm methods.

    bool IsRelevantDecisionMade() const;

    bool IsSuitableTimeForCompaction(TInstant currentTime) const;

    void MakeDecision(TInstant timestamp, EStoreCompactionReason reason);

protected:
    template <class TRecalculator>
    bool DoRecalculateHint(TRecalculator&& recalculator, TRange<std::unique_ptr<TStore>> stores);
};

////////////////////////////////////////////////////////////////////////////////

//! Lives permanently in sorted stores in tablet node
//! Lives temporarily in LSM stores created in interop.
class TStoreCompactionHint
    : public TCompactionHintBase
{
public:
    using TChunkViewTooNarrowPayload = double;
    using TVersionedRowDigestPayload = NTableClient::TVersionedRowDigestPtr;
    using TMinHashDigestPayload = TMinHashDigestPtr;

    using TPayload = std::variant<std::monostate, TChunkViewTooNarrowPayload, TVersionedRowDigestPayload, TMinHashDigestPayload>;

public:
    using TCompactionHintBase::TCompactionHintBase;

    // Should be called in LSM to isolate logic from tablet node.
    bool RecalculateHint(const std::unique_ptr<TStore>& store);
};

// NB(dave11ar): Not virtual function of TStoreCompactionHint to avoid allocations and save memory.
template <EStoreCompactionHintKind Kind>
void DoRecalculateStoreCompactionHint(TStore* store) = delete;

////////////////////////////////////////////////////////////////////////////////

//! Manage interaction with all kinds of store compaction hints, also store payloads.
//! Lives temporarily in LSM stores created in interop.
class TStoreCompactionHints
{
public:
    using TPayloads = TStoreCompactionHintArray<TStoreCompactionHint::TPayload>;
    using THints = TCalculatableStoreCompactionHintArray<TStoreCompactionHint>;

public:
    DEFINE_BYREF_RW_PROPERTY(TPayloads, Payloads);
    DEFINE_BYREF_RW_PROPERTY(THints, Hints);

public:
    EStoreCompactionReason GetStoreCompactionReason(TInstant currentTime) const;

    bool RecalculateHints(const std::unique_ptr<TStore>& store);
};

////////////////////////////////////////////////////////////////////////////////

//! Lives permanently in partition in tablet node.
//! Lives temporarily in LSM partitions created in interop.
class TPartitionCompactionHint
    : public TCompactionHintBase
{
public:
    DEFINE_BYREF_RO_PROPERTY(std::vector<TStoreId>, StoreIds);

    using TCompactionHintBase::TCompactionHintBase;

    // Should be called in LSM to isolate logic from tablet node.
    bool RecalculateHint(TPartition* partition);
};

// NB(dave11ar): Not virtual function of TPartitionCompactionHint for avoiding allocations.
template <EPartitionCompactionHintKind Kind>
void DoRecalculatePartitionCompactionHint(TPartition* partition) = delete;

////////////////////////////////////////////////////////////////////////////////

//! Manage interaction with all kinds of partition compaction hints.
//! Lives temporarily in LSM partitionss created in interop.
class TPartitionCompactionHints
{
public:
    using THints = TPartitionCompactionHintArray<TPartitionCompactionHint>;

public:
    DEFINE_BYREF_RW_PROPERTY(THints, Hints);

public:
    TPartitionCompactionHints() = default;
    TPartitionCompactionHints(THints hints);

    std::pair<EStoreCompactionReason, std::vector<TStoreId>> GetStoresForCompaction(
        TInstant currentTime,
        TTimestamp edenMajorTimestamp) const;

    bool RecalculateHints(TPartition* partition);
};

////////////////////////////////////////////////////////////////////////////////

TCompactionHintUpdateRequest RecalculateCompactionHints(TTablet* tablet);

////////////////////////////////////////////////////////////////////////////////

void Serialize(
    const TStoreCompactionHint& storeCompactionHint,
    NYson::IYsonConsumer* consumer);

void Serialize(
    const TPartitionCompactionHint& partitionCompactionHint,
    NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
