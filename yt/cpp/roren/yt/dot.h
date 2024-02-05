#pragma once

#include <util/stream/output.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

void DOTPrologue(IOutputStream& out);

TString DOTTableName(int tableId);
void DOTTable(IOutputStream& out, const TString& name, const TString& path);

void DOTEdge(IOutputStream& out, const TString& from, const TString& to);

TString DOTOperationName(int operationId);
void DOTOperation(IOutputStream& out, const TString& name, const TString& label);

void DOTEpilogue(IOutputStream& out);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
