#pragma once

#include <yt/ytlib/hydra/public.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/client/tablet_client/public.h>

namespace NYT {
namespace NTabletClient {

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
);

////////////////////////////////////////////////////////////////////////////////

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

} // namespace NTabletClient
} // namespace NYT

