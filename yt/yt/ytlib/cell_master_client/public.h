#pragma once

#include "public.h"

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NYT::NCellMasterClient {

///////////////////////////////////////////////////////////////////////////////

using TSecondaryMasterConnectionConfigs = THashMap<NObjectClient::TCellTag, NApi::NNative::TMasterConnectionConfigPtr>;

///////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ICellDirectory)
DECLARE_REFCOUNTED_STRUCT(ICellDirectorySynchronizer)

DECLARE_REFCOUNTED_CLASS(TCellDirectoryConfig)
DECLARE_REFCOUNTED_CLASS(TCellDirectorySynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMasterClient
