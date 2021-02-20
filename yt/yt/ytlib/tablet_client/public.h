#pragma once

#include <yt/ytlib/hydra/public.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/client/tablet_client/public.h>

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReqRegisterTransactionActions;
class TRspRegisterTransactionActions;
class TTableReplicaStatistics;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETabletBackgroundActivity,
    ((Compaction)     (0))
    ((Flush)          (1))
    ((Partitioning)   (2))
    ((Preload)        (3))
    ((Rotation)       (4))
);

DEFINE_ENUM(ETabletCellLifeStage,
    (Running)
    (DecommissioningOnMaster)
    (DecommissioningOnNode)
    (Decommissioned)
);

DEFINE_ENUM(ETabletStoresUpdateReason,
    ((Unknown)          (0))
    ((Flush)            (1))
    ((Compaction)       (2))
    ((Partitioning)     (3))
    ((Trim)             (4))
);

////////////////////////////////////////////////////////////////////////////////

constexpr int TypicalTabletSlotCount = 10;
constexpr int TypicalPeerCount = 5;
constexpr int MaxPeerCount = 10;
constexpr int MaxTabletCount = 10000;

constexpr int MaxDynamicMemoryPoolWeight = 1000;
// NB: The product of maximum node memory limit, pool weight and tablet slot count
// must not overflow int64. We estimate memory limit with 1T and tablet slot count
// with 20.
static_assert(
    (1ull << 63) / (1ll << 40) / 20 >= MaxDynamicMemoryPoolWeight,
    "MaxDynamicMemoryPoolWeight is too large");

//! $tablet_index and $row_index.
constexpr int OrderedTabletSystemColumnCount = 2;

//! Signatures enable checking tablet transaction integrity.
/*!
 *  When a transaction is created, its signature is #InitialTransactionSignature.
 *  Each change within a transaction is annotated with a signature; these signatures are
 *  added to the transaction's signature. For a commit to be successful, the final signature must
 *  be equal to #FinalTransactionSignature.
 */
using TTransactionSignature = ui32;
const TTransactionSignature InitialTransactionSignature = 0;
const TTransactionSignature FinalTransactionSignature = 0xffffffffU;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTabletCellOptions)
DECLARE_REFCOUNTED_CLASS(TDynamicTabletCellOptions)
DECLARE_REFCOUNTED_CLASS(TTabletCellConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient

