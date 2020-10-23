#pragma once

#include "private.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/client/transaction_client/public.h>

#include <yt/client/ypath/rich.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TPartitionedTableHarvesterOptions
{
    NYPath::TRichYPath RichPath;
    NApi::NNative::IClientPtr Client;
    // May be nullptr, in which case attributes are fetched directly.
    NObjectClient::TObjectAttributeCachePtr ObjectAttributeCache;
    NTransactionClient::TTransactionId TransactionId;
    IInvokerPtr Invoker;

    TPartitionedTableHarvesterConfigPtr Config;

    NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

class TPartitionedTableHarvester
    : public TRefCounted
{
public:
    class TImpl;

    explicit TPartitionedTableHarvester(TPartitionedTableHarvesterOptions options);
    ~TPartitionedTableHarvester();

    //! Fetch and validate all necessary meta including partition
    //! schemas and boundary keys, but do not fetch chunks.
    TFuture<void> Prepare();

    //! Leave only partitions satisfying given predicate.
    void FilterPartitions(std::function<bool(TLegacyKey, TLegacyKey)> predicate);

    //! Fetch chunk specs and return table read spec.
    TFuture<TTableReadSpec> Fetch(const TFetchSingleTableReadSpecOptions& options);

private:
    using TImplPtr = TIntrusivePtr<TImpl>;
    TImplPtr Impl_;
};

DEFINE_REFCOUNTED_TYPE(TPartitionedTableHarvester);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
