#include "stdafx.h"
#include "sync_writer.h"
#include "table_consumer.h"
#include "config.h"

#include <ytlib/misc/string.h>

namespace NYT {
namespace NTableClient {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void TTableConsumer::OnStringScalar(const TStringBuf& value)
{
    if (ControlState == EControlState::ExpectControlAttributeValue) {
        YCHECK(Depth == 1);
        ThrowInvalidControlAttribute("be a string value");
    }

    YCHECK(ControlState == EControlState::None);

    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ValueWriter.OnStringScalar(value);
    }
}

void TTableConsumer::OnIntegerScalar(i64 value)
{
    if (ControlState == EControlState::ExpectControlAttributeValue) {
        YCHECK(Depth == 1);

        switch (ControlAttribute) {
            case EControlAttribute::TableIndex: {
                if (value < 0 || value >= Writers.size()) {
                    THROW_ERROR_EXCEPTION(
                        "Invalid table index: expected in range [0, %d], actual %" PRId64,
                        static_cast<int>(Writers.size()) - 1,
                        value)
                        << TErrorAttribute("row_index", Writer->GetRowCount());
                }
                CurrentTableIndex = value;
                Writer = Writers[CurrentTableIndex];
                ControlState = EControlState::ExpectEndControlAttributes;
                break;
            }

            default:
                ThrowInvalidControlAttribute("be an integer value");
        }

        return;
    }

    YCHECK(ControlState == EControlState::None);

    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ValueWriter.OnIntegerScalar(value);
    }
}

void TTableConsumer::OnDoubleScalar(double value)
{
    if (ControlState == EControlState::ExpectControlAttributeValue) {
        YCHECK(Depth == 1);
        ThrowInvalidControlAttribute("be a double value");
    }

    YCHECK(ControlState == EControlState::None);

    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ValueWriter.OnDoubleScalar(value);
    }
}

void TTableConsumer::OnEntity()
{
    switch (ControlState) {
    case EControlState::None:
        break;

    case EControlState::ExpectEntity:
        YCHECK(Depth == 0);
        // Successfully processed control statement.
        ControlState = EControlState::None;
        return;

    case EControlState::ExpectControlAttributeValue:
        ThrowInvalidControlAttribute("be an entity");
        break;

    default:
        YUNREACHABLE();
    };


    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ValueWriter.OnEntity();
    }
}

void TTableConsumer::OnBeginList()
{
    if (ControlState == EControlState::ExpectControlAttributeValue) {
        YCHECK(Depth == 1);
        ThrowInvalidControlAttribute("be a list");
    }

    YCHECK(ControlState == EControlState::None);

    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ++Depth;
        ValueWriter.OnBeginList();
    }
}

void TTableConsumer::OnBeginAttributes()
{
    if (ControlState == EControlState::ExpectControlAttributeValue) {
        YCHECK(Depth == 1);
        ThrowInvalidControlAttribute("have attributes");
    }

    YCHECK(ControlState == EControlState::None);

    if (Depth == 0) {
        ControlState = EControlState::ExpectControlAttributeName;
    } else {
        ValueWriter.OnBeginAttributes();
    }

    ++Depth;
}

void TTableConsumer::OnListItem()
{
    YCHECK(ControlState == EControlState::None);

    if (Depth == 0) {
        // Row separator, do nothing.
    } else {
        ValueWriter.OnListItem();
    }
}

void TTableConsumer::OnBeginMap()
{
    if (ControlState == EControlState::ExpectControlAttributeValue) {
        YCHECK(Depth == 1);
        ThrowInvalidControlAttribute("be a map");
    }

    if (ControlState == EControlState::ExpectEntity) {
        ThrowEntityExpected();
    }

    YCHECK(ControlState == EControlState::None);

    if (Depth > 0) {
        ValueWriter.OnBeginMap();
    }

    ++Depth;
}

void TTableConsumer::OnKeyedItem(const TStringBuf& name)
{
    switch (ControlState) {
    case EControlState::None:
        break;

    case EControlState::ExpectControlAttributeName:
        YCHECK(Depth == 1);
        try {
            ControlAttribute = ParseEnum<EControlAttribute>(ToString(name));
        } catch (const std::exception&) {
            // Ignore ex, our custom message is more meaningful.
            THROW_ERROR_EXCEPTION("Failed to parse control attribute name %s",
                ~Stroka(name).Quote());
        }
        ControlState = EControlState::ExpectControlAttributeValue;
        return;

    case EControlState::ExpectEndControlAttributes:
        YCHECK(Depth == 1);
        THROW_ERROR_EXCEPTION("Too many control attributes per record: at most one attribute is allowed");
        break;

    default:
        YUNREACHABLE();
    };

    YCHECK(Depth > 0);
    if (Depth == 1) {
        Offsets.push_back(RowBuffer.Size());
        RowBuffer.Write(name);

        Offsets.push_back(RowBuffer.Size());
    } else {
        ValueWriter.OnKeyedItem(name);
    }
}

void TTableConsumer::OnEndMap()
{
    YCHECK(Depth > 0);
    // No control attribute allows map or composite values.
    YCHECK(ControlState == EControlState::None);

    --Depth;

    if (Depth > 0) {
        ValueWriter.OnEndMap();
        return;
    }

    YCHECK(Offsets.size() % 2 == 0);

    TRow row;
    row.reserve(Offsets.size() / 2);

    int index = Offsets.size();
    int begin = RowBuffer.Size();
    while (index > 0) {
        int end = begin;
        begin = Offsets[--index];
        TStringBuf value(RowBuffer.Begin() + begin, end - begin);

        end = begin;
        begin = Offsets[--index];
        TStringBuf name(RowBuffer.Begin() + begin, end - begin);

        row.push_back(std::make_pair(name, value));
    }

    Writer->WriteRow(row);

    Offsets.clear();
    RowBuffer.Clear();
}

void TTableConsumer::OnEndList()
{
   // No control attribute allow list or composite values.
    YCHECK(ControlState == EControlState::None);

    --Depth;
    YCHECK(Depth > 0);
    ValueWriter.OnEndList();
}

void TTableConsumer::OnEndAttributes()
{
    --Depth;

    switch (ControlState) {
        case EControlState::ExpectControlAttributeName:
            THROW_ERROR_EXCEPTION("Too few control attributes per record: at least one attribute is required");
            break;

        case EControlState::ExpectEndControlAttributes:
            YCHECK(Depth == 0);
            ControlState = EControlState::ExpectEntity;
            break;

        case EControlState::None:
            YCHECK(Depth > 0);
            ValueWriter.OnEndAttributes();
            break;

        default:
            YUNREACHABLE();
    };
}

void TTableConsumer::OnRaw(const TStringBuf& yson, EYsonType type)
{
    YUNREACHABLE();
}

void TTableConsumer::ThrowMapExpected() const
{
    ThrowError("Invalid row format, map expected");
}

void TTableConsumer::ThrowEntityExpected() const
{
    ThrowError("Invalid row format, there is control attributes, entity expected");
}

void TTableConsumer::ThrowInvalidControlAttribute(const Stroka& whatsWrong) const
{
    ThrowError(
        Sprintf("Control attribute %s cannot %s",
            ~FormatEnum(ControlAttribute).Quote(),
            ~whatsWrong));
}

void TTableConsumer::ThrowError(const Stroka& message) const
{
    THROW_ERROR_EXCEPTION(message)
        << TErrorAttribute("table_index", CurrentTableIndex)
        << TErrorAttribute("row_index", Writer->GetRowCount());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
