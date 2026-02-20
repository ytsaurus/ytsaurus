#pragma once

#include <contrib/ydb/core/base/defs.h>
#include <contrib/ydb/core/wrappers/s3_storage_config.h>

#include <util/generic/string.h>

namespace NKikimr::NReplication::NService {

IActor* CreateS3Writer(NWrappers::IExternalStorageConfig::TPtr&& s3Settings, const TString& tableName, const TString& writerName);

}
