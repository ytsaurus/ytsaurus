#pragma once

#include "cypress_object_repository.h"
#include "private.h"

#include <yt/yt/core/ytree/convert.h>

#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

#include <string>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TYtDatabaseBase
    : public DB::IDatabase
{
public:
    TYtDatabaseBase(String databaseName, THost* host);

    void createTable(
        const DB::ContextPtr /*context*/,
        const std::string& /*name*/,
        const DB::StoragePtr& table,
        const DB::ASTPtr& /*query*/) override;

    void shutdown() override;

    bool isTableExist(const String& name, DB::ContextPtr context) const override;

    DB::StoragePtr tryGetTable(const String& name, DB::ContextPtr context) const override;

    bool empty() const override;

    bool canContainMergeTreeTables() const override;

    bool canContainDistributedTables() const override;

    DB::ASTPtr getCreateDatabaseQuery() const override;

    void dropTable(DB::ContextPtr context, const String& name, bool /*noDelay*/) override;

    void renameTable(
        DB::ContextPtr context,
        const String& name,
        IDatabase& /*toDatabase*/,
        const String& toName,
        bool exchange,
        bool dictionary) override;

    DB::ASTPtr getCreateTableQueryImpl(const String& name, DB::ContextPtr context, bool throwOnError) const override;

protected:
    THost* const Host_;

    DB::StoragePtr DoGetTable(DB::ContextPtr context, const String& name) const;

    DB::StoragePtr DoGetYTTable(DB::ContextPtr context, TQueryContext* queryContext, const DB::StorageID& storageId) const;

    DB::StoragePtr DoGetChytObject(
        DB::ContextPtr context,
        TQueryContext* queryContext,
        const DB::StorageID& storageId,
        std::optional<TRepositoryObjectDescriptor>* resolvedObject) const;
    DB::StoragePtr DoGetDictionary(DB::ContextPtr context, TQueryContext* queryContext, const DB::StorageID& storageId) const;
    DB::StoragePtr DoGetMaterializedView(
        DB::ContextPtr context,
        const DB::StorageID& storageId,
        std::optional<TCypressObjectRepository::TMaterializedView>* resolvedView = nullptr) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
