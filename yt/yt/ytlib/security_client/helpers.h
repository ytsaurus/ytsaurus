#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

THashSet<std::string> GetSubjectClosure(
    const std::string& subject,
    NObjectClient::TObjectServiceProxy& proxy,
    const NApi::NNative::IConnectionPtr& connection,
    const NApi::TMasterReadOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
