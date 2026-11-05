#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/payload.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Reduces |hash| to a tablet index in [0, tabletCount) according to |policy|.
i64 ReduceHashToTabletIndex(ui64 hash, i64 tabletCount, EQueueTabletIndexRoutingHashPolicy policy);

////////////////////////////////////////////////////////////////////////////////

//! Routes queue rows to tablets by evaluating a QL expression (builtins only,
//! e.g. #farm_hash) over the message payload via YT's column evaluator.
//! Carries a reusable scratch row/buffer, so a single instance must be used by
//! one thread at a time (the sink drives it from its serialized commit path).
class TTabletIndexEvaluator
    : public TRefCounted
{
public:
    //! |policy| unset: the expression value is used as the tablet index verbatim.
    //! |policy| set: the expression yields a uint64 hash reduced by |policy| and the tablet count.
    //! Throws if the expression's result type is not uint64.
    TTabletIndexEvaluator(
        const NTableClient::TTableSchemaPtr& streamSchema,
        const std::string& expression,
        std::optional<EQueueTabletIndexRoutingHashPolicy> policy);

    //! Computes the tablet index for |payload| given the live |tabletCount|; throws on a non-uint64
    //! result or a tablet index outside [0, tabletCount).
    i64 GetTabletIndex(const TPayload& payload, i64 tabletCount);

private:
    struct TReference
    {
        int EvalSchemaId;
        int PayloadColumnId;
    };

    const std::optional<EQueueTabletIndexRoutingHashPolicy> Policy_;
    const std::string Expression_;

    NQueryClient::TColumnEvaluatorPtr Evaluator_;
    int TabletIndexColumnId_;
    std::vector<TReference> References_;

    // Reused across #GetTabletIndex calls to avoid per-message allocation.
    // |Row_| is backed by |RowBuffer_| (never cleared); |EvalBuffer_| is the
    // scratch buffer for #EvaluateKeys output.
    const NTableClient::TRowBufferPtr RowBuffer_;
    const NTableClient::TRowBufferPtr EvalBuffer_;
    NTableClient::TMutableUnversionedRow Row_;
};

DEFINE_REFCOUNTED_TYPE(TTabletIndexEvaluator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
