#pragma once

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/yson/public.h>

#include <library/cpp/yt/yson_string/string.h>

#include <memory>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

//! Creates a streaming consumer that expects a YSON list and forwards into
//! #underlying the projection of #postAsteriskPath out of every list element
//! (an entity is emitted for elements that miss the path).
std::unique_ptr<NYson::IYsonConsumer> CreateAsteriskProjectionConsumer(
    NYson::IYsonConsumer* underlying,
    NYPath::TYPathBuf postAsteriskPath);

//! Projects #postAsteriskPath out of every element of a YSON list.
//! A thin convenience wrapper around #CreateAsteriskProjectionConsumer.
NYson::TYsonString ProjectListAsterisk(
    const NYson::TYsonString& listYson,
    NYPath::TYPathBuf postAsteriskPath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
