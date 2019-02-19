#pragma once

#include "private.h"
#include "bootstrap.h"
#include "public.h"
#include "public_ch.h"

#include "document.h"
#include "objects.h"
#include "path.h"
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

struct TTableObject
    : public NChunkClient::TUserObject
{
    int ChunkCount = 0;
    bool Dynamic = false;
    NTableClient::TTableSchema Schema;
};

////////////////////////////////////////////////////////////////////////////////

struct TQueryContext
    : public DB::IHostContext
{
public:
    TLogger Logger;
    TString User;
    TQueryId QueryId;

    explicit TQueryContext(TBootstrap* bootstrap, const TString& user, TQueryId queryId)
        : Logger(ServerLogger)
        , User(user)
        , QueryId(queryId)
        , Bootstrap_(bootstrap)
    {
        Logger.AddTag("QueryId: %v", queryId);
        YT_LOG_INFO("Host context created (User: %v)", User);
    }

    ~TQueryContext()
    {
        YT_LOG_INFO("Host context destroyed");
    }

    NApi::NNative::IClientPtr& Client()
    {
        ClientLock_.AcquireReader();
        auto clientPresent = static_cast<bool>(Client_);
        ClientLock_.ReleaseReader();

        if (!clientPresent) {
            ClientLock_.AcquireWriter();
            Client_ = Bootstrap_->GetClientCache()->GetClient(User);
            ClientLock_.ReleaseWriter();
        }

        return Client_;
    }

    // Related services

    const IPathService* PathService();

    // Access data / metadata

    std::vector<TTablePtr> ListTables(
        const TString& path = {},
        bool recursive = false);

    TTablePtr GetTable(const TString& name);

    std::vector<TTablePtr> GetTables(const TString& jobSpec);

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
        const TTableReaderOptions& options);

    ITableReaderPtr CreateTableReader(const TString& name, const TTableReaderOptions& options);

    TString ReadFile(const TString& name);

    IDocumentPtr ReadDocument(const TString& name);

    bool Exists(const TString& name);

    TObjectList ListObjects(const TString& path);

    TObjectAttributes GetObjectAttributes(const TString& path);

    NYTree::IMapNodePtr GetAttributes(const TString& path, const std::vector<TString>& attributes);

    // We still need this for effective polling through metadata cache
    // TODO: replace by CreateObjectPoller

    std::optional<TRevision> GetObjectRevision(const TString& name, bool throughCache);

private:
    TBootstrap* Bootstrap_;

    TReaderWriterSpinLock ClientLock_;

    //! Native client for the user that initiated the query. Created on first use.
    NApi::NNative::IClientPtr Client_;

    std::unique_ptr<TTableObject> GetTableAttributes(
        NApi::NNative::ITransactionPtr transaction,
        const NYPath::TRichYPath& path,
        NYTree::EPermission permission,
        bool suppressAccessTracking);
};

////////////////////////////////////////////////////////////////////////////////

void SetupHostContext(TBootstrap* bootstrap, DB::Context& context);

TQueryContext* GetQueryContext(const DB::Context& context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
