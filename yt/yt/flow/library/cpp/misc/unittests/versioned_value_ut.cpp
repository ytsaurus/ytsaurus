#include <yt/yt/flow/library/cpp/misc/versioned_value.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

using TVersionedInt = TVersionedValue<int>;

constexpr ui64 FirstUnixTime = 1'784'633'264;
constexpr ui64 SecondUnixTime = 1'784'633'265;

TVersion VersionAt(ui64 unixTime, i64 counter)
{
    return TVersion(
        static_cast<i64>(NTransactionClient::TimestampFromUnixTime(unixTime).Underlying()) + counter);
}

class TStubVersionProvider
    : public IVersionProvider
{
public:
    explicit TStubVersionProvider(TVersion version)
        : Version_(version)
    { }

    void SetNext(TVersion version)
    {
        Version_ = version;
    }

    TVersion GenerateVersion() override
    {
        return Version_;
    }

private:
    TVersion Version_;
};

class TThrowingVersionProvider
    : public IVersionProvider
{
public:
    TVersion GenerateVersion() override
    {
        THROW_ERROR_EXCEPTION("Injected failure");
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST(TVersionedValueTest, ChangedValueAdoptsTheVersion)
{
    auto value = New<TVersionedInt>();
    EXPECT_EQ(value->GetVersion(), TVersion(0));

    auto version = VersionAt(FirstUnixTime, 7);
    auto provider = New<TStubVersionProvider>(version);
    EXPECT_TRUE(value->TrySetValue(42, provider));
    EXPECT_EQ(value->GetValue(), 42);
    EXPECT_EQ(value->GetVersion(), version);
    EXPECT_EQ(value->GetLastUpdate(), TInstant::Seconds(FirstUnixTime));
}

TEST(TVersionedValueTest, EqualValueKeepsTheVersion)
{
    auto value = New<TVersionedInt>();
    auto version = VersionAt(FirstUnixTime, 7);
    auto provider = New<TStubVersionProvider>(version);
    EXPECT_TRUE(value->TrySetValue(42, provider));

    provider->SetNext(VersionAt(SecondUnixTime, 1));
    EXPECT_FALSE(value->TrySetValue(42, provider));
    EXPECT_EQ(value->GetVersion(), version);
    EXPECT_EQ(value->GetLastUpdate(), TInstant::Seconds(FirstUnixTime));
}

TEST(TVersionedValueTest, ProviderFailureLeavesStateUnchanged)
{
    auto value = New<TVersionedInt>();
    auto version = VersionAt(FirstUnixTime, 7);
    EXPECT_TRUE(value->TrySetValue(42, New<TStubVersionProvider>(version)));

    EXPECT_THROW(value->TrySetValue(43, New<TThrowingVersionProvider>()), TErrorException);
    EXPECT_EQ(value->GetValue(), 42);
    EXPECT_EQ(value->GetVersion(), version);
    EXPECT_EQ(value->GetLastUpdate(), TInstant::Seconds(FirstUnixTime));
}

TEST(TVersionedValueTest, BumpAdoptsTheVersion)
{
    auto value = New<TVersionedInt>();
    auto provider = New<TStubVersionProvider>(VersionAt(FirstUnixTime, 7));
    value->TrySetValue(42, provider);

    auto version = VersionAt(SecondUnixTime, 1);
    provider->SetNext(version);
    value->Bump(provider);
    EXPECT_EQ(value->GetVersion(), version);
    EXPECT_EQ(value->GetValue(), 42);
    EXPECT_EQ(value->GetLastUpdate(), TInstant::Seconds(SecondUnixTime));
}

TEST(TVersionedValueDeathTest, BumpRejectsANonIncreasingVersion)
{
    auto value = New<TVersionedInt>();
    auto version = VersionAt(SecondUnixTime, 1);
    auto provider = New<TStubVersionProvider>(version);
    value->TrySetValue(42, provider);

    EXPECT_DEATH(value->Bump(provider), ".*");
    provider->SetNext(VersionAt(FirstUnixTime, 7));
    EXPECT_DEATH(value->Bump(provider), ".*");
}

TEST(TVersionedValueTest, VersionAndLastUpdateSurviveYsonRoundTrip)
{
    auto value = New<TVersionedInt>();
    auto version = VersionAt(FirstUnixTime, 7);
    auto provider = New<TStubVersionProvider>(version);
    value->TrySetValue(42, provider);

    auto node = ConvertToNode(value);
    EXPECT_TRUE(node->AsMap()->FindChild("last_update"));

    auto restored = ConvertTo<TIntrusivePtr<TVersionedInt>>(node);
    EXPECT_EQ(restored->GetVersion(), version);
    EXPECT_EQ(restored->GetValue(), 42);
    EXPECT_EQ(restored->GetLastUpdate(), TInstant::Seconds(FirstUnixTime));
}

TEST(TVersionedValueTest, LegacyCounterIsSupersededByAClockVersion)
{
    auto value = ConvertTo<TIntrusivePtr<TVersionedInt>>(BuildYsonNodeFluently()
            .BeginMap()
            .Item("version")
            .Value(57)
            .Item("value")
            .Value(42)
            .EndMap());
    EXPECT_EQ(value->GetVersion(), TVersion(57));
    EXPECT_EQ(value->GetLastUpdate(), TInstant::Zero());

    auto version = VersionAt(FirstUnixTime, 7);
    auto provider = New<TStubVersionProvider>(version);
    EXPECT_TRUE(value->TrySetValue(43, provider));
    EXPECT_EQ(value->GetVersion(), version);
    EXPECT_EQ(value->GetLastUpdate(), TInstant::Seconds(FirstUnixTime));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
