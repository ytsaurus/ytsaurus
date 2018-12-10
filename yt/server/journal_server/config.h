#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NJournalServer {

////////////////////////////////////////////////////////////////////////////////

class TJournalManagerConfig
    : public NYTree::TYsonSerializable
{ };

DEFINE_REFCOUNTED_TYPE(TJournalManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::ТJournalServer
