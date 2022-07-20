#include <library/cpp/testing/gtest/gtest.h>

#include <yt/yt/core/misc/format.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

static_assert(!TFormatTraits<TIntrusivePtr<TRefCounted>>::HasCustomFormatValue);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
