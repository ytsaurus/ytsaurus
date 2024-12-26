#include <yt/yt/library/fusion/service_directory.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFusion {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct IRawPtrService
{ };

class TRawPtrService
    : public IRawPtrService
{ };

TEST(TServiceDirectoryTest, RawPtrService)
{
    auto impl = std::make_unique<TRawPtrService>();
    auto serviceDirectory = CreateServiceDirectory();
    serviceDirectory->RegisterService<IRawPtrService*>(impl.get());
    auto* service = serviceDirectory->GetServiceOrThrow<IRawPtrService*>();
    EXPECT_EQ(service, impl.get());
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IIntrusivePtrService)

struct IIntrusivePtrService
    : public TRefCounted
{ };


DEFINE_REFCOUNTED_TYPE(IIntrusivePtrService)

class TIntrusivePtrService
    : public IIntrusivePtrService
{ };

TEST(TServiceDirectoryTest, IntrusivePtrService)
{
    auto impl = New<TIntrusivePtrService>();
    auto serviceDirectory = CreateServiceDirectory();
    serviceDirectory->RegisterService<IIntrusivePtrServicePtr>(impl);
    auto service = serviceDirectory->GetServiceOrThrow<IIntrusivePtrServicePtr>();
    EXPECT_EQ(service, impl);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TServiceDirectoryTest, UnregisteredService)
{
    auto serviceDirectory = CreateServiceDirectory();
    EXPECT_EQ(serviceDirectory->FindService<IRawPtrService*>(), nullptr);
    EXPECT_THROW_WITH_SUBSTRING(
        serviceDirectory->GetServiceOrThrow<IRawPtrService*>(),
        "is not registered");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFusion
