#include <gtest/gtest.h>

#include <yt/yt/library/di/tagged_ptr.h>

#include <library/cpp/yt/memory/new.h>

namespace NYT::NDI {
namespace {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TFoo)

struct TFoo
    : public TRefCounted
{ };

DEFINE_REFCOUNTED_TYPE(TFoo)

DEFINE_ENUM(ETestEnum,
    (Foo)
    (Bar)
);

TEST(TTaggedPtr, TaggedPtr)
{
    TTaggedPtr<TFoo, ETestEnum::Foo> test{New<TFoo>()};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NDI
