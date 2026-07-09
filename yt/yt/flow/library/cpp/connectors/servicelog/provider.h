#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IServiceLogRowsProvider);

struct IServiceLogRowsProvider
    : public TRefCounted
{
    using TPayloadRow = NTableClient::TUnversionedOwningRow;

    struct TFetchResult
    {
        std::vector<TPayloadRow> Rows;
        bool Finished;
    };

    virtual NTableClient::TTableSchemaPtr GetSchema() = 0;
    //! Returns rows and whether the traversal is over.
    virtual TFuture<TFetchResult> Fetch(const TServiceLogRangePtr& range, i64 rowLimit) = 0;
    //! Important: provider's row with the given key can't be omitted if it exists.
    //! So, if we can't fetch it due to failures or in some other rare conditions (see TestProtoBaseTTableJoinerTest::SpecialCasesWithRightCritical)
    //! we should throw error instead of possibly skipping key.

    virtual i64 GetApproximateRowCount() = 0;
};

DEFINE_REFCOUNTED_TYPE(IServiceLogRowsProvider);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
