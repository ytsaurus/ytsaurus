#pragma once

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/yson/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Build build (pun intended) attributes as a YSON string a-la /orchid/service. If service name is not provided,
//! it is omitted from the result.
void BuildBuildAttributes(NYson::IYsonConsumer* consumer, const char* serviceName = nullptr);

void SetBuildAttributes(NYTree::IYPathServicePtr orchidRoot, const char* serviceName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
