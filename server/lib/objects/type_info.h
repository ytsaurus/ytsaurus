#pragma once

#include "public.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

// E.g. "pod set", "DNS record set", "multi-cluster replica set".
TStringBuf GetHumanReadableTypeName(EObjectType type);

// E.g. "Pod set", "DNS record set", "Multi-cluster replica set".
TStringBuf GetCapitalizedHumanReadableTypeName(EObjectType type);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
