#pragma once

#include "public.h"

#include <yt/yt/ytlib/sequoia_client/sequoia_reign.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

TError CheckSequoiaReign(NSequoiaClient::ESequoiaReign requestReign);
void ValidateSequoiaReign(NSequoiaClient::ESequoiaReign requestReign);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
