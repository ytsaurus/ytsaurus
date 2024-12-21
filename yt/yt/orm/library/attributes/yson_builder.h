#pragma once

#include <yt/yt/core/yson/public.h>

#include <library/cpp/yt/misc/strong_typedef.h>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

struct IYsonBuilder
{
    YT_DEFINE_STRONG_TYPEDEF(TCheckpoint, int);

    virtual ~IYsonBuilder() = default;

    virtual NYson::IYsonConsumer* GetConsumer() = 0;
    virtual TCheckpoint CreateCheckpoint() = 0;
    virtual void RestoreCheckpoint(TCheckpoint checkpoint) = 0;

    NYson::IYsonConsumer* operator->();
};

////////////////////////////////////////////////////////////////////////////////

class TYsonStringBuilder
    : public IYsonBuilder
{
public:
    TYsonStringBuilder(
        NYson::EYsonFormat format = NYson::EYsonFormat::Binary,
        NYson::EYsonType type = NYson::EYsonType::Node,
        bool enableRaw = true);

    NYson::IYsonConsumer* GetConsumer() override;
    IYsonBuilder::TCheckpoint CreateCheckpoint() override;
    void RestoreCheckpoint(IYsonBuilder::TCheckpoint checkpoint) override;

    NYson::TYsonString Flush();
    bool IsEmpty();

private:
    TString ValueString_;
    TStringOutput Output_;
    const std::unique_ptr<NYson::IFlushableYsonConsumer> Writer_;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EYsonBuilderForwardingPolicy,
    (Forward)
    (Ignore)
    (Crash)
);

////////////////////////////////////////////////////////////////////////////////

class TYsonBuilder
    : public IYsonBuilder
{
public:
    TYsonBuilder(EYsonBuilderForwardingPolicy policy, IYsonBuilder* underlying, NYson::IYsonConsumer* consumer);

    NYson::IYsonConsumer* GetConsumer() override;
    IYsonBuilder::TCheckpoint CreateCheckpoint() override;
    void RestoreCheckpoint(TCheckpoint checkpoint) override;

private:
    const EYsonBuilderForwardingPolicy Policy_;
    IYsonBuilder* const Underlying_;
    NYson::IYsonConsumer* const Consumer_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
