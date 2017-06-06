#pragma once

#include <mapreduce/yt/interface/client.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TString GetOperationPath(const TOperationId& operationId);
TString GetOperationStatusPath(const TOperationId& operation);

EOperationStatus CheckOperation(
    NYT::IClientBasePtr clientBasePtr,
    const TOperationId& operationId);

EOperationStatus CheckOperationStatus(
    NYT::IClientBasePtr clientBasePtr,
    const TOperationId& operationId,
    const TString& state);

void DumpOperationStderrs(
    NYT::IClientBasePtr client,
    TOutputStream& stream,
    const TString& operationPath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
