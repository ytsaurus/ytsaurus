#include "public.h"

#include <yt/yt/client/table_client/columnar_statistics.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TTableColumnarStatisticsCache
    : public TRefCounted
{
public:
    TTableColumnarStatisticsCache(
        TTableColumnarStatisticsCacheConfigPtr config,
        NApi::NNative::IClientPtr client,
        IInvokerPtr invoker,
        NLogging::TLogger logger,
        NProfiling::TProfiler profiler);
    ~TTableColumnarStatisticsCache();

    //! Represents a request for aggregate columnar statistics for `Path`. Statistics are guaranteed to correspond
    //! to at least `MinRevision` revision. In case of cache miss, `Schema` is used to determine which columns
    //! should be requested.
    //! NB: this cache believes `Schema` to be the actual table schema. In other words, if cached
    //! information fits given minimum revision, it is returned despite the fact it may not contain all of the
    //! requested columns.
    struct TRequest
    {
        NObjectClient::TObjectId ObjectId;
        NObjectClient::TCellTag ExternalCellTag;
        i64 ChunkCount;
        const NTableClient::TTableSchemaPtr Schema;
        NHydra::TRevision MinRevision;
    };

    TFuture<std::vector<TErrorOr<TNamedColumnarStatistics>>> GetFreshStatistics(std::vector<TRequest> requests);

private:
    class TImpl;

    TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TTableColumnarStatisticsCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
