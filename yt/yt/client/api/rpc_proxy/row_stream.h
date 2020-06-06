#pragma once

#include "public.h"

#include <yt/client/api/rpc_proxy/proto/api_service.pb.h>

#include <yt/client/api/public.h>

#include <yt/client/table_client/unversioned_row_batch.h>

#include <yt/core/misc/ref.h>
#include <yt/core/misc/range.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

struct IRowStreamFormatter
    : public virtual TRefCounted
{
    virtual TSharedRef Format(
        const NTableClient::IUnversionedRowBatchPtr& batch,
        const NProto::TRowsetStatistics* statistics) = 0;
};

DEFINE_REFCOUNTED_TYPE(IRowStreamFormatter)

////////////////////////////////////////////////////////////////////////////////

struct IRowStreamParser
    : public virtual TRefCounted
{
    virtual TSharedRange<NTableClient::TUnversionedRow> Parse(
        const TSharedRef& block,
        NProto::TRowsetStatistics* statistics) = 0;
};

DEFINE_REFCOUNTED_TYPE(IRowStreamParser)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
