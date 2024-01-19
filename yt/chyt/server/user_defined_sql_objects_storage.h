#pragma once

#include "private.h"

#include <yt/yt/client/hydra/public.h>

#include <Functions/UserDefined/IUserDefinedSQLObjectsStorage.h>
#include <Interpreters/Context_fwd.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

struct TSqlObjectInfo
{
    NHydra::TRevision Revision;
    TString CreateObjectQuery;
};

void ToProto(NProto::TSqlObjectInfo* protoInfo, const TSqlObjectInfo& info);
void FromProto(TSqlObjectInfo* info, const NProto::TSqlObjectInfo& protoInfo);

////////////////////////////////////////////////////////////////////////////////

struct IUserDefinedSqlObjectsYTStorage
{
    virtual bool TrySetObject(const String& objectName, const TSqlObjectInfo& info) = 0;

    virtual bool TryRemoveObject(const String& objectName, NHydra::TRevision revision) = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IUserDefinedSQLObjectsStorage> CreateUserDefinedSqlObjectsYTStorage(
    DB::ContextPtr globalContext,
    TUserDefinedSqlObjectsStorageConfigPtr config,
    THost* host);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
