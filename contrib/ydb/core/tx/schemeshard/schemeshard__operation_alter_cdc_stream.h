#pragma once

#include "schemeshard__operation_common.h"
#include "schemeshard__operation_create_cdc_stream.h" // for TStreamPaths
#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#include <contrib/ydb/core/engine/mkql_proto.h>
#include <contrib/ydb/core/scheme/scheme_types_proto.h>

namespace NKikimr::NSchemeShard::NCdc {

std::variant<TStreamPaths, ISubOperation::TPtr> DoAlterStreamPathChecks(
    const TOperationId& opId,
    const TPath& workingDirPath,
    const TString& tableName,
    const TString& streamName);

void DoAlterStream(
    TVector<ISubOperation::TPtr>& result,
    const NKikimrSchemeOp::TAlterCdcStream& op,
    const TOperationId& opId,
    const TPath& workingDirPath,
    const TPath& tablePath);

} // namespace NKikimr::NSchemesShard::NCdc
