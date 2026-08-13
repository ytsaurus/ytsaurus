#pragma once

#include "private.h"

#include <yt/yt/core/ypath/public.h>

#include <Parsers/ASTCreateQuery.h>
#include <Storages/IStorage.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

struct IStorageYtMaterializedView
{
    virtual IStorageDistributorPtr ResolveTargetDistributor(DB::ContextPtr context) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TMaterializedViewConfiguration
{
    std::string CreateStatement;
    NYPath::TYPath SourcePath;
    NYPath::TYPath TargetPath;
    NObjectClient::TObjectId SourceObjectId;
    NObjectClient::TObjectId TargetObjectId;
};

TMaterializedViewConfiguration BuildMaterializedViewConfiguration(
    const DB::ContextPtr& context,
    const DB::StoragePtr& table,
    const DB::ASTPtr& query);

////////////////////////////////////////////////////////////////////////////////

//! CHYT-side counterpart of DB::StorageMaterializedView (TO-form only, no refresh).
//! The native storage resolves its target through the global context, which carries
//! no user identity: the target would be fetched under the root client, bypassing
//! the ACLs. This storage resolves the target with the query context.
DB::StoragePtr CreateStorageYtMaterializedView(
    const DB::StorageID& storageId,
    const DB::ASTCreateQuery& createQuery,
    NYPath::TYPath targetPath,
    DB::ContextPtr context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
