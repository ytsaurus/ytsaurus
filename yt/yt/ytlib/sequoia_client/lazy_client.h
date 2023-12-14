#pragma once

#include "client.h"

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

//! This client enables setting (and even replacing) the actual underlying ground client
//! at later times. All invocations are initially postponed until a ground client
//! is provided.
struct ILazySequoiaClient
    : public ISequoiaClient
{
    virtual void SetGroundClient(const NApi::NNative::IClientPtr& groundClient) = 0;
};

DEFINE_REFCOUNTED_TYPE(ILazySequoiaClient)

////////////////////////////////////////////////////////////////////////////////

ILazySequoiaClientPtr CreateLazySequoiaClient(
    NApi::NNative::IClientPtr nativeClient,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
