#pragma once

#include "private.h"
#include "bootstrap.h"
#include "host.h"

#include "document.h"
#include "objects.h"
#include "system_columns.h"
#include "table_reader.h"
#include "table_partition.h"
#include "table_schema.h"

#include <yt/ytlib/api/native/client_cache.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/core/concurrency/public.h>
#include <yt/core/concurrency/rw_spinlock.h>
#include <yt/core/logging/log.h>

#include <Interpreters/Context.h>

namespace NYT::NClickHouseServer {

using TStringList = std::vector<TString>;

using namespace NLogging;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

struct TObjectListItem
{
    TString Name;
    TObjectAttributes Attributes;
};

using TObjectList = std::vector<TObjectListItem>;

////////////////////////////////////////////////////////////////////////////////

struct TQueryContext
    : public DB::IHostContext
{
public:
    TLogger Logger;
    TString User;
    TQueryId QueryId;
    EQueryKind QueryKind;

    explicit TQueryContext(TBootstrap* bootstrap, TQueryId queryId, const DB::Context& context);

    ~TQueryContext();

    const NApi::NNative::IClientPtr& Client() const;

    // TODO(max42): helpers below should not belong to query context. Maybe move them to helpers.h?
    std::vector<TClickHouseTablePtr> ListTables(
        const TString& path = {},
        bool recursive = false);

    TTablePartList GetTableParts(const TString& name, const DB::KeyCondition* keyCondition, size_t maxParts = 1);
    TTablePartList GetTablesParts(const std::vector<TString>& names, const DB::KeyCondition* keyCondition, size_t maxParts = 1);

    TTablePartList ConcatenateAndGetTableParts(
        const std::vector<TString>& names,
        const DB::KeyCondition* keyCondition = nullptr,
        size_t maxParts = 1);

    TTableReaderList CreateTableReaders(
        const TString& jobSpec,
        const TStringList& columns,
        const TSystemColumns& systemColumns,
        size_t maxStreamCount,
        bool unordered);

    bool Exists(const TString& name);

private:
    TBootstrap* Bootstrap_;
    TClickHouseHostPtr Host_;

    //! Spinlock controlling lazy client creation.
    mutable TReaderWriterSpinLock ClientLock_;

    //! Native client for the user that initiated the query. Created on first use.
    mutable NApi::NNative::IClientPtr Client_;
};

////////////////////////////////////////////////////////////////////////////////

void SetupHostContext(TBootstrap* bootstrap, DB::Context& context, TQueryId queryId = TQueryId());

TQueryContext* GetQueryContext(const DB::Context& context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
