#include "experimental_yson_pull_format.h"

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/value_consumer.h>
#include <yt/yt/client/table_client/unversioned_value.h>

#include <yt/yt/core/misc/coro_pipe.h>
#include <yt/yt/core/yson/pull_parser.h>

#include <type_traits>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

using namespace NYson;
using namespace NTableClient;

class TYsonPullParserImpl {
public:
    TYsonPullParserImpl(IZeroCopyInput* stream, IValueConsumer* valueConsumer)
        : Parser_(stream, NYson::EYsonType::ListFragment)
        , ValueConsumer_(valueConsumer)
        , NameTableWriter_(valueConsumer->GetNameTable())
    { }

    void Parse()
    {
        while (ParseRow()) {
        }
    }

    bool ParseRow()
    {
        auto event = Parser_.Next();
        if (event.GetType() == EYsonItemType::EndOfStream) {
            return false;
        } else if (event.GetType() != EYsonItemType::BeginMap) {
            THROW_ERROR_EXCEPTION("Expected map, got: %v", event.GetType());
        }
        event = Parser_.Next();
        ValueConsumer_->OnBeginRow();
        while (true) {
            if (event.GetType() == EYsonItemType::EndMap) {
                break;
            }
            YT_ASSERT(event.GetType() == EYsonItemType::StringValue);

            const auto& columnId = NameTableWriter_.GetIdOrRegisterName(event.UncheckedAsString());
            event = Parser_.Next();
            TUnversionedValue unversionedValue;
            switch (event.GetType()) {
                case EYsonItemType::EntityValue:
                    unversionedValue = MakeUnversionedNullValue(columnId);
                    break;
                case EYsonItemType::BooleanValue:
                    unversionedValue = MakeUnversionedDoubleValue(event.UncheckedAsBoolean(), columnId);
                    break;
                case EYsonItemType::Int64Value:
                    unversionedValue = MakeUnversionedInt64Value(event.UncheckedAsInt64(), columnId);
                    break;
                case EYsonItemType::Uint64Value:
                    unversionedValue = MakeUnversionedUint64Value(event.UncheckedAsUint64(), columnId);
                    break;
                case EYsonItemType::DoubleValue:
                    unversionedValue = MakeUnversionedDoubleValue(event.UncheckedAsDouble(), columnId);
                    break;
                case EYsonItemType::StringValue:
                    unversionedValue = MakeUnversionedStringValue(event.UncheckedAsString(), columnId);
                    break;
                default:
                    THROW_ERROR_EXCEPTION("Unimplemented");
            }
            ValueConsumer_->OnValue(unversionedValue);
            event = Parser_.Next();
        }
        ValueConsumer_->OnEndRow();
        return true;
    }

private:
    NYson::TYsonPullParser Parser_;
    IValueConsumer* ValueConsumer_;
    TNameTableWriter NameTableWriter_;
};

////////////////////////////////////////////////////////////////////////////////

class TYsonPullParser
    : public IParser
{
public:
    TYsonPullParser(IValueConsumer* valueConsumer)
        : CoroPipe_(BIND([=] (IZeroCopyInput* stream) {
            TYsonPullParserImpl parser(stream, valueConsumer);
            parser.Parse();
        }))
    { }

    void Read(TStringBuf data) override
    {
        CoroPipe_.Feed(data);
    }

    void Finish() override
    {
        CoroPipe_.Finish();
    }

private:
    TCoroPipe CoroPipe_;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForYsonPull(
    NTableClient::IValueConsumer* consumer,
    TYsonFormatConfigPtr /*config*/,
    int /*tableIndex*/)
{
    return std::make_unique<TYsonPullParser>(consumer);
}

} // namespace NYT::NFormats
