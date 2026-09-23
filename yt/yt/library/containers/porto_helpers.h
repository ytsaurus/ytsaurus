#pragma once

#include <yt/yt/core/misc/error.h>

#include <util/generic/strbuf.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf DefaultPortoNetworkInterface = "veth";

////////////////////////////////////////////////////////////////////////////////

//! This is a wildcard pattern that matches everything.
constexpr const char* AnyTarget = "***";
//! This is a wildcard pattern that matches everything.
constexpr const char* AnyContainer = "***";

////////////////////////////////////////////////////////////////////////////////

TError CheckPortoContainerNameComponent(TStringBuf component);
void ValidatePortoContainerNameComponent(TStringBuf component);

TError CheckPortoContainerSubpath(TStringBuf subpath);
void ValidatePortoContainerSubpath(TStringBuf subpath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
