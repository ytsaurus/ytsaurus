#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/companion/companion_model.h>
#include <yt/yt/flow/library/cpp/companion/state_codec.h>

#include <yt/yt/flow/library/cpp/companion/proto/companion_service.pb.h>

#include <yt/yt/flow/library/cpp/common/key.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/yt/misc/cast.h>

namespace NYT::NFlow::NCompanion {
namespace {

////////////////////////////////////////////////////////////////////////////////

TKey MakeTestKey()
{
    return MakeKey(ui64{42});
}

NProto::NCompanion::TState MakeProtoState(
    TStringBuf name,
    bool reset,
    TStringBuf payload,
    std::optional<EStateFormat> format = std::nullopt)
{
    NProto::NCompanion::TState protoState;
    protoState.set_name(TProtobufString(name));
    if (format) {
        protoState.set_format(ToUnderlying(*format));
    }
    auto* protoItem = protoState.add_stateitems();
    NYT::ToProto(protoItem->mutable_key(), MakeTestKey());
    protoItem->set_reset(reset);
    if (!reset) {
        protoItem->set_state(TProtobufString(payload));
    }
    return protoState;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStateCodecTest, ParseNonResetEmptyPayloadThrows)
{
    auto protoState = MakeProtoState("counter", /*reset*/ false, /*payload*/ "");
    EXPECT_THROW_WITH_SUBSTRING(
        ParseStateHolder<std::string>(protoState, EStateDirection::Response),
        "Empty state value");
}

TEST(TStateCodecTest, ParseAcceptsEmptyPayloadInRequest)
{
    auto protoState = MakeProtoState("counter", /*reset*/ false, /*payload*/ "");
    auto holder = ParseStateHolder<std::string>(protoState, EStateDirection::Request);
    ASSERT_EQ(std::ssize(holder.StateItems), 1);
    EXPECT_TRUE(holder.StateItems[0].State.empty());
}

TEST(TStateCodecTest, ParseAcceptsEmptyPayloadOnReset)
{
    auto protoState = MakeProtoState("counter", /*reset*/ true, /*payload*/ "");
    auto holder = ParseStateHolder<std::string>(protoState, EStateDirection::Response);
    ASSERT_EQ(std::ssize(holder.StateItems), 1);
    EXPECT_TRUE(holder.StateItems[0].Reset);
}

TEST(TStateCodecTest, ParseAcceptsEmptyProtoPayload)
{
    // The exemption is keyed on the format carried by the wire message, not on
    // the receiver's own knowledge of the state; internal states never set a
    // format and thus can never take it.
    auto protoState = MakeProtoState(
        "profile",
        /*reset*/ false,
        /*payload*/ "",
        EStateFormat::Proto);
    auto holder = ParseStateHolder<TSharedRef>(protoState, EStateDirection::Response);
    EXPECT_EQ(holder.Format, EStateFormat::Proto);
    ASSERT_EQ(std::ssize(holder.StateItems), 1);
    EXPECT_TRUE(holder.StateItems[0].State.Empty());
}

TEST(TStateCodecTest, ParseKeepsNonEmptyPayload)
{
    auto protoState = MakeProtoState("counter", /*reset*/ false, /*payload*/ "payload");
    auto holder = ParseStateHolder<std::string>(protoState, EStateDirection::Response);
    EXPECT_EQ(holder.StateName, "counter");
    ASSERT_EQ(std::ssize(holder.StateItems), 1);
    EXPECT_EQ(holder.StateItems[0].Key, MakeTestKey());
    EXPECT_FALSE(holder.StateItems[0].Reset);
    EXPECT_EQ(holder.StateItems[0].State, "payload");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanion
