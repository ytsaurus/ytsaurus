#pragma once

#include "private.h"

#include <Storages/IStorage_fwd.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

struct TStorageYtDirOptions
{
    std::optional<TString> From;
    std::optional<TString> To;
    bool TablesOnly = false;
    bool ResolveLinks = false;
};

DB::StoragePtr CreateStorageYtDir(TString dirPath, TStorageYtDirOptions options);

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageYtNodeAttributes(std::vector<TString> paths);

////////////////////////////////////////////////////////////////////////////////

struct TStorageYtLogTablesOptions
{
    std::optional<TInstant> From;
    std::optional<TInstant> To;
};

DB::StoragePtr CreateStorageYtLogTables(TString logPath, TStorageYtLogTablesOptions options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
