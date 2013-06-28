#include "stdafx.h"
#include "sync_writer.h"
#include "table_consumer.h"
#include "config.h"

#include <ytlib/misc/string.h>

namespace NYT {
namespace NTableClient {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TTableConsumer::TTableConsumer(ISyncWriterPtr writer)
    : ControlState(EControlState::None)
    , CurrentTableIndex(0)
    , Writer(writer)
    , Depth(0)
    , ValueWriter(&RowBuffer)
{
    Writers.push_back(writer);
}

TTableConsumer::TTableConsumer(const std::vector<ISyncWriterPtr>& writers, int tableIndex)
    : ControlState(EControlState::None)
    , CurrentTableIndex(tableIndex)
    , Writers(writers)
    , Writer(Writers[CurrentTableIndex])
    , Depth(0)
    , ValueWriter(&RowBuffer)
{ }

void TTableConsumer::OnStringScalar(const TStringBuf& value)
{
    if (ControlState == EControlState::ExpectValue) {
        YASSERT(Depth == 1);
        ThrowIncorrectAttributeValueType("string scalar");
    }

    YASSERT(ControlState == EControlState::None);

    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ValueWriter.OnStringScalar(value);
    }
}

void TTableConsumer::OnIntegerScalar(i64 value)
{
    if (ControlState == EControlState::ExpectValue) {
        YASSERT(Depth == 1);

        switch (ControlAttribute) {
            case EControlAttributes::TableIndex: {
                if (value >= Writers.size()) {
                    THROW_ERROR_EXCEPTION(
                        "Invalid table index (TableIndex: %d, TableCount: %d, CurrentTableIndex: %d, RowIndex: %" PRId64 ")",
                        value,
                        static_cast<int>(Writers.size()),
                        CurrentTableIndex,
                        Writer->GetRowCount());
                }
                CurrentTableIndex = value;
                Writer = Writers[CurrentTableIndex];
                ControlState = EControlState::ExpectEndAttributes;
                break;
            }

            default:
                ThrowIncorrectAttributeValueType("integer scalar");
        }

        return;
    }

    YASSERT(ControlState == EControlState::None);

    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ValueWriter.OnIntegerScalar(value);
    }
}

void TTableConsumer::OnDoubleScalar(double value)
{
    if (ControlState == EControlState::ExpectValue) {
        YASSERT(Depth == 1);
        ThrowIncorrectAttributeValueType("double scalar");
    }

    YASSERT(ControlState == EControlState::None);

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
        YASSERT(Depth == 0);
        // Successfully processed control statement.
        ControlState = EControlState::None;
        return;

    case EControlState::ExpectValue:
        ThrowIncorrectAttributeValueType("entity");
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
    if (ControlState == EControlState::ExpectValue) {
        YASSERT(Depth == 1);
        ThrowIncorrectAttributeValueType("list");
    }

    YASSERT(ControlState == EControlState::None);

    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ++Depth;
        ValueWriter.OnBeginList();
    }
}

void TTableConsumer::OnBeginAttributes()
{
    if (ControlState == EControlState::ExpectValue) {
        YASSERT(Depth == 1);
        ThrowIncorrectAttributeValueType("value with attributes");
    }

    YASSERT(ControlState == EControlState::None);

    if (Depth == 0) {
        ControlState = EControlState::ExpectName;
    } else {
        ValueWriter.OnBeginAttributes();
    }

    ++Depth;
}

void TTableConsumer::ThrowMapExpected()
{
    THROW_ERROR_EXCEPTION(
        "Invalid row format, map expected (TableIndex: %d, RowIndex: %" PRId64 ")",
        CurrentTableIndex,
        Writer->GetRowCount());
}

void TTableConsumer::ThrowIncorrectAttributeValueType(const Stroka& type)
{
    THROW_ERROR_EXCEPTION(
        "Incorrect value type %s for control attribute %s (TableIndex: %d, RowIndex: %" PRId64 ")",
        ~type.Quote(),
        ~FormatEnum(ControlAttribute),
        CurrentTableIndex,
        Writer->GetRowCount());
}

void TTableConsumer::OnListItem()
{
    YASSERT(ControlState == EControlState::None);

    if (Depth == 0) {
        // Row separator, do nothing.
    } else {
        ValueWriter.OnListItem();
    }
}

void TTableConsumer::OnBeginMap()
{
    if (ControlState == EControlState::ExpectValue) {
        YASSERT(Depth == 1);
        ThrowIncorrectAttributeValueType("map");
    }

    YASSERT(ControlState == EControlState::None);

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

    case EControlState::ExpectName:
        YASSERT(Depth == 1);
        try {
            ControlAttribute = ParseEnum<EControlAttributes>(ToString(name));
            ControlState = EControlState::ExpectValue;
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to parse control attribute name") << ex;
        }
        return;

    case EControlState::ExpectEndAttributes:
        YASSERT(Depth == 1);
        THROW_ERROR_EXCEPTION("Too many control attributes: maximum 1 attribute is allowed");
        break;

    default:
        YUNREACHABLE();

    };

    YASSERT(Depth > 0);

    if (Depth == 1) {
        Offsets.push_back(RowBuffer.GetSize());
        RowBuffer.Write(name);

        Offsets.push_back(RowBuffer.GetSize());
    } else {
        ValueWriter.OnKeyedItem(name);
    }
}

void TTableConsumer::OnEndMap()
{
    YASSERT(Depth > 0);
    // No control attribute allow map or composite values.
    YASSERT(ControlState == EControlState::None);

    --Depth;

    if (Depth > 0) {
        ValueWriter.OnEndMap();
        return;
    }

    YASSERT(Offsets.size() % 2 == 0);

    TRow row;
    row.reserve(Offsets.size() / 2);

    int index = Offsets.size();
    int begin = RowBuffer.GetSize();
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
    YASSERT(ControlState == EControlState::None);

    --Depth;
    YASSERT(Depth > 0);
    ValueWriter.OnEndList();
}

void TTableConsumer::OnEndAttributes()
{
    --Depth;

    switch (ControlState) {
    case EControlState::ExpectName:
        THROW_ERROR_EXCEPTION("Empty control attributes are not allowed");
        break;

    case EControlState::ExpectEndAttributes:
        YASSERT(Depth == 0);
        ControlState = EControlState::ExpectEntity;
        break;

    case EControlState::None:
        YASSERT(Depth > 0);
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
