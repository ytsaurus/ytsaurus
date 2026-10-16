#include "asterisk_projection.h"

#include "attribute_path.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/yson/forwarding_consumer.h>
#include <yt/yt/core/yson/parser.h>
#include <yt/yt/core/yson/ypath_designated_consumer.h>
#include <yt/yt/core/yson/yson_builder.h>

namespace NYT::NOrm::NAttributes {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TAsteriskProjectionConsumer
    : public NYson::TForwardingYsonConsumer
{
public:
    TAsteriskProjectionConsumer(NYson::IYsonConsumer* underlying, NYPath::TYPathBuf postAsteriskPath)
        : Underlying_(underlying)
        , ElementConsumer_(NYson::CreateYPathDesignatedConsumer(
            NYPath::TYPath(postAsteriskPath),
            NYson::EMissingPathMode::EmitEntity,
            underlying))
    { }

private:
    NYson::IYsonConsumer* const Underlying_;
    const std::unique_ptr<NYson::IFlushableYsonConsumer> ElementConsumer_;

    [[noreturn]] static void ThrowNotAList()
    {
        THROW_ERROR_EXCEPTION(NAttributes::EErrorCode::InvalidData,
            "Asterisk can only project lists");
    }

    void OnMyBeginList() override
    {
        Underlying_->OnBeginList();
    }

    void OnMyListItem() override
    {
        Underlying_->OnListItem();
        Forward(
            ElementConsumer_.get(),
            [this] { ElementConsumer_->Flush(); },
            NYson::EYsonType::Node);
    }

    void OnMyEndList() override
    {
        Underlying_->OnEndList();
    }

    void OnMyEntity() override
    {
        Underlying_->OnEntity();
    }

    void OnMyBeginMap() override
    {
        THROW_ERROR_EXCEPTION(NAttributes::EErrorCode::Unimplemented,
            "Asterisk projection over maps is not supported yet");
    }

    void OnMyBeginAttributes() override
    {
        ThrowNotAList();
    }

    void OnMyStringScalar(TStringBuf /*value*/) override
    {
        ThrowNotAList();
    }

    void OnMyInt64Scalar(i64 /*value*/) override
    {
        ThrowNotAList();
    }

    void OnMyUint64Scalar(ui64 /*value*/) override
    {
        ThrowNotAList();
    }

    void OnMyDoubleScalar(double /*value*/) override
    {
        ThrowNotAList();
    }

    void OnMyBooleanScalar(bool /*value*/) override
    {
        ThrowNotAList();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NYson::IYsonConsumer> CreateAsteriskProjectionConsumer(
    NYson::IYsonConsumer* underlying,
    NYPath::TYPathBuf postAsteriskPath)
{
    THROW_ERROR_EXCEPTION_IF(PathContainsAsterisk(postAsteriskPath),
        "Double asterisk projection is not supported");
    return std::make_unique<TAsteriskProjectionConsumer>(underlying, postAsteriskPath);
}

NYson::TYsonString ProjectListAsterisk(
    const NYson::TYsonString& listYson,
    NYPath::TYPathBuf postAsteriskPath)
{
    NYson::TYsonStringBuilder builder;
    auto consumer = CreateAsteriskProjectionConsumer(builder.GetConsumer(), postAsteriskPath);
    NYson::ParseYsonStringBuffer(listYson.AsStringBuf(), NYson::EYsonType::Node, consumer.get());
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
