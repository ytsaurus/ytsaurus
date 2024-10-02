#pragma once

#include "public.h"

#include <yt/yt/orm/server/master/bootstrap.h>

#include <yt/yt/client/api/public.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct IObjectTableReader
    : public TRefCounted
{
    // If batching is enabled, the rows returned from both |StreamAll| and |Read| methods
    // may contain extra key fields at the end.

    struct TStreamResult
    {
        NYT::NApi::IUnversionedRowsetPtr Rowset;
        TDuration ResponseDelay;
    };

    // Do not support batching yet.
    virtual std::vector<TFuture<TStreamResult>> StreamAll(
        NObjects::TTimestamp timestamp,
        NObjects::TTransactionManagerPtr transactionManager,
        IInvokerPtr invoker) = 0;

    virtual NYT::NApi::IUnversionedRowsetPtr Read(
        NObjects::ISession* session,
        std::source_location location = std::source_location::current()) = 0;

    virtual bool IsFinished() const = 0;
    virtual void Reset() = 0;

    virtual void SetCustomObjectFilter(TString filter) = 0;
    virtual void ResetCustomObjectFilter() = 0;
};

DEFINE_REFCOUNTED_TYPE(IObjectTableReader)

////////////////////////////////////////////////////////////////////////////////

IObjectTableReaderPtr CreateObjectTableReader(
    NMaster::IBootstrap* bootstrap,
    TObjectTableReaderConfigPtr config,
    const TDBTable* dbTable,
    std::vector<TString> fieldNames,
    TString removalTimeFieldName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
