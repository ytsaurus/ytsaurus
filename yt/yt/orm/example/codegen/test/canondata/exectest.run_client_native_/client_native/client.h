// AUTOMATICALLY GENERATED. DO NOT EDIT!

#pragma once

#include "public.h"

#include <yt/yt/orm/client/native/client.h>

namespace NYT::NOrm::NExample::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

//! Token will be taken from env EXAMPLE_TOKEN.
//! If the EXAMPLE_TOKEN was empty, token will be taken from ~/.example/token.
TString FindToken();

NYT::NOrm::NClient::NNative::IClientPtr CreateClient(
    TConnectionConfigPtr config,
    NYT::NOrm::NClient::NNative::TClientOptions clientOptions = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NClient::NNative
