#pragma once

#include "public.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/client/api/client.h>

#include <yt/client/misc/public.h>

#include <yt/core/ypath/public.h>

namespace NYT::NCypressClient {

////////////////////////////////////////////////////////////////////////////////

// Attributes will be fetched from cypress.
TFuture<std::vector<TErrorOr<NYTree::TAttributeMap>>> FetchAttributes(
    const std::vector<NYPath::TYPath>& paths,
    const std::vector<TString>& attributes,
    const NApi::NNative::IClientPtr& client,
    const NApi::TMasterReadOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

} // NYT::NCypressClient
