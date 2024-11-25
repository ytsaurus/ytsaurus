#pragma once

#include "private.h"

#include <yt/yt/core/actions/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

//! Class that keeps information about all currently running queries for given server context.
/*!
 *  Invoker affinity: `invoker`
 */
class TQueryRegistry
    : public TRefCounted
{
public:
    TQueryRegistry(IInvokerPtr invoker, DB::ContextPtr context, TQueryRegistryConfigPtr config);
    ~TQueryRegistry();

    void Register(TQueryContextPtr queryContext);
    void Unregister(TQueryContextPtr queryContext);

    void AccountPhaseCounter(TQueryContextPtr queryContext, EQueryPhase fromPhase, EQueryPhase toPhase);

    //! Invoker affinity: any.
    void AccountPhaseDuration(EQueryPhase phase, TDuration duration);
    //! Invoker affinity: any.
    void AccountTotalDuration(TDuration duration);

    size_t GetQueryCount() const;
    TFuture<void> GetIdleFuture() const;

    TFuture<std::optional<TQueryProgressValues>> GetQueryProgress(TQueryId queryId) const;

    NYTree::IYPathServicePtr GetOrchidService() const;

    void WriteStateToStderr() const;

    void Start();
    void Stop();

    //! Returns stored query finish info for every query id.
    //! The query finish info is deleted from the query registry after the extraction.
    //! Invoker affinity: any.
    TFuture<std::vector<std::optional<TQueryFinishInfo>>> ExtractQueryFinishInfos(const std::vector<TQueryId>& queryIds);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TQueryRegistry)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
