#pragma once

#include "public.h"

#include <yt/core/yson/public.h>

#include <yt/core/ytree/public.h>

#include <contrib/libs/re2/re2/re2.h>

namespace NYT {
namespace NRe2 {

using namespace re2;

////////////////////////////////////////////////////////////////////////////////

// We create a ref-counted version of re2 to deal with an issue of regular re2::RE2
// being not default-constructible which is not convenient when using regexps in
// Yson-serializable configs.

//! Ref-counted version of re2::RE2.
class TRe2
    : public RE2
    , public TRefCounted
{
    using RE2::RE2;
};

DEFINE_REFCOUNTED_TYPE(TRe2)

// NB: we cannot use raw RE2 as a field in yson serializable because RE2 is not
// default-constructible. Thus we have to overload unique_ptr version of it.
void Serialize(const TRe2Ptr& re, NYson::IYsonConsumer* consumer);
void Deserialize(TRe2Ptr& re, NYTree::INodePtr& node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRe2
} // namespace NYT