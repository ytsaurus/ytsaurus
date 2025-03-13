#pragma once

#include <contrib/ydb/core/ydb_convert/table_profiles.h>
#include <contrib/ydb/core/ydb_convert/table_settings.h>

namespace NKikimr {
namespace NGRpcService {

bool FillCreateTableSettingsDesc(NKikimrSchemeOp::TTableDescription& out,
    const Ydb::Table::CreateTableRequest& in, const TTableProfiles& profiles,
    Ydb::StatusIds::StatusCode& code, TString& error, TList<TString>& warnings);


} // namespace NGRpcService
} // namespace NKikimr
