#include "stdafx.h"
#include "async_writer.h"
#include "table_consumer.h"
#include "config.h"

#include <core/misc/string.h>

#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/schemaless_writer.h>
#include <ytlib/new_table_client/unversioned_row.h>

#include <core/concurrency/scheduler.h>

namespace NYT {
namespace NTableClient {

using namespace NConcurrency;
using namespace NYson;
using namespace NVersionedTableClient;

const i64 MaxValueBufferSize = 10 * 1024 * 1024;

////////////////////////////////////////////////////////////////////////////////

void TLegacyTableConsumer::OnStringScalar(const TStringBuf& value)
{
    if (ControlState == ELegacyTableConsumerControlState::ExpectControlAttributeValue) {
        YCHECK(Depth == 1);
        ThrowInvalidControlAttribute("be a string value");
    }

    YCHECK(ControlState == ELegacyTableConsumerControlState::None);

    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ValueWriter.OnStringScalar(value);
    }
}

void TLegacyTableConsumer::OnInt64Scalar(i64 value)
{
    if (ControlState == ELegacyTableConsumerControlState::ExpectControlAttributeValue) {
        YCHECK(Depth == 1);

        switch (ControlAttribute) {
            case EControlAttribute::TableIndex: {
                if (value < 0 || value >= Writers.size()) {
                    THROW_ERROR_EXCEPTION(
                        "Invalid table index: expected in range [0, %v], actual %v",
                        Writers.size() - 1,
                        value)
                        << TErrorAttribute("row_index", Writer->GetRowCount());
                }
                CurrentTableIndex = value;
                Writer = Writers[CurrentTableIndex];
                ControlState = ELegacyTableConsumerControlState::ExpectEndControlAttributes;
                break;
            }

            default:
                ThrowInvalidControlAttribute("be an integer value");
        }

        return;
    }

    YCHECK(ControlState == ELegacyTableConsumerControlState::None);

    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ValueWriter.OnInt64Scalar(value);
    }
}

void TLegacyTableConsumer::OnUint64Scalar(ui64 value)
{
    if (ControlState == ELegacyTableConsumerControlState::ExpectControlAttributeValue) {
        YCHECK(Depth == 1);
        ThrowInvalidControlAttribute("be a uint64 value");
    }

    YCHECK(ControlState == ELegacyTableConsumerControlState::None);

    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ValueWriter.OnUint64Scalar(value);
    }
}

void TLegacyTableConsumer::OnDoubleScalar(double value)
{
    if (ControlState == ELegacyTableConsumerControlState::ExpectControlAttributeValue) {
        YCHECK(Depth == 1);
        ThrowInvalidControlAttribute("be a double value");
    }

    YCHECK(ControlState == ELegacyTableConsumerControlState::None);

    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ValueWriter.OnDoubleScalar(value);
    }
}

void TLegacyTableConsumer::OnBooleanScalar(bool value)
{
    YCHECK(ControlState == ELegacyTableConsumerControlState::None);

    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ValueWriter.OnBooleanScalar(value);
    }
}

void TLegacyTableConsumer::OnEntity()
{
    switch (ControlState) {
        case ELegacyTableConsumerControlState::None:
            break;

        case ELegacyTableConsumerControlState::ExpectEntity:
            YCHECK(Depth == 0);
            // Successfully processed control statement.
            ControlState = ELegacyTableConsumerControlState::None;
            return;

	    case ELegacyTableConsumerControlState::ExpectControlAttributeValue:
            ThrowInvalidControlAttribute("be an entity");
            break;

        default:
            YUNREACHABLE();
    }


    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ValueWriter.OnEntity();
    }
}

void TLegacyTableConsumer::OnBeginList()
{
    if (ControlState == ELegacyTableConsumerControlState::ExpectControlAttributeValue) {
        YCHECK(Depth == 1);
        ThrowInvalidControlAttribute("be a list");
    }

    YCHECK(ControlState == ELegacyTableConsumerControlState::None);

    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ++Depth;
        ValueWriter.OnBeginList();
    }
}

void TLegacyTableConsumer::OnBeginAttributes()
{
    if (ControlState == ELegacyTableConsumerControlState::ExpectControlAttributeValue) {
        YCHECK(Depth == 1);
        ThrowInvalidControlAttribute("have attributes");
    }

    YCHECK(ControlState == ELegacyTableConsumerControlState::None);

    if (Depth == 0) {
        ControlState = ELegacyTableConsumerControlState::ExpectControlAttributeName;
    } else {
        ValueWriter.OnBeginAttributes();
    }

    ++Depth;
}

void TLegacyTableConsumer::OnListItem()
{
    YCHECK(ControlState == ELegacyTableConsumerControlState::None);

    if (Depth == 0) {
        // Row separator, do nothing.
    } else {
        ValueWriter.OnListItem();
    }
}

void TLegacyTableConsumer::OnBeginMap()
{
    if (ControlState == ELegacyTableConsumerControlState::ExpectControlAttributeValue) {
        YCHECK(Depth == 1);
        ThrowInvalidControlAttribute("be a map");
    }

    if (ControlState == ELegacyTableConsumerControlState::ExpectEntity) {
        ThrowEntityExpected();
    }

    YCHECK(ControlState == ELegacyTableConsumerControlState::None);

    if (Depth > 0) {
        ValueWriter.OnBeginMap();
    }

    ++Depth;
}

void TLegacyTableConsumer::OnKeyedItem(const TStringBuf& name)
{
    switch (ControlState) {
   	    case ELegacyTableConsumerControlState::None:
            break;

        case ELegacyTableConsumerControlState::ExpectControlAttributeName:
            YCHECK(Depth == 1);
            try {
                ControlAttribute = ParseEnum<EControlAttribute>(ToString(name));
            } catch (const std::exception&) {
                // Ignore ex, our custom message is more meaningful.
                THROW_ERROR_EXCEPTION("Failed to parse control attribute name %Qv",
                    name);
            }
            ControlState = ELegacyTableConsumerControlState::ExpectControlAttributeValue;
            return;

        case ELegacyTableConsumerControlState::ExpectEndControlAttributes:
            YCHECK(Depth == 1);
            THROW_ERROR_EXCEPTION("Too many control attributes per record: at most one attribute is allowed");
            break;

        default:
            YUNREACHABLE();
    }

    YCHECK(Depth > 0);
    if (Depth == 1) {
        Offsets.push_back(RowBuffer.Size());
        RowBuffer.Write(name);

        if (RowBuffer.Size() > NTableClient::MaxRowWeightLimit) {
            THROW_ERROR_EXCEPTION(
                "Row weight is too large (%v > %v)",
                RowBuffer.Size(),
                NTableClient::MaxRowWeightLimit);
        }

        Offsets.push_back(RowBuffer.Size());
    } else {
        ValueWriter.OnKeyedItem(name);
    }
}

void TLegacyTableConsumer::OnEndMap()
{
    YCHECK(Depth > 0);
    // No control attribute allows map or composite values.
    YCHECK(ControlState == ELegacyTableConsumerControlState::None);

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

void TLegacyTableConsumer::OnEndList()
{
   // No control attribute allow list or composite values.
    YCHECK(ControlState == ELegacyTableConsumerControlState::None);

    --Depth;
    YCHECK(Depth > 0);
    ValueWriter.OnEndList();
}

void TLegacyTableConsumer::OnEndAttributes()
{
    --Depth;

    switch (ControlState) {
        case ELegacyTableConsumerControlState::ExpectControlAttributeName:
            THROW_ERROR_EXCEPTION("Too few control attributes per record: at least one attribute is required");
            break;

        case ELegacyTableConsumerControlState::ExpectEndControlAttributes:
            YCHECK(Depth == 0);
            ControlState = ELegacyTableConsumerControlState::ExpectEntity;
            break;

        case ELegacyTableConsumerControlState::None:
            YCHECK(Depth > 0);
            ValueWriter.OnEndAttributes();
            break;

        default:
            YUNREACHABLE();
    };
}

void TLegacyTableConsumer::OnRaw(const TStringBuf& yson, EYsonType type)
{
    YCHECK(ControlState == ELegacyTableConsumerControlState::None);
    YCHECK(Depth > 0);
    YCHECK(type == EYsonType::Node);

    ValueWriter.OnRaw(yson, type);
}

void TLegacyTableConsumer::ThrowMapExpected() const
{
    ThrowError("Invalid row format, map expected");
}

void TLegacyTableConsumer::ThrowEntityExpected() const
{
    ThrowError("Invalid row format, there are control attributes, entity expected");
}

void TLegacyTableConsumer::ThrowInvalidControlAttribute(const Stroka& whatsWrong) const
{
    ThrowError(Format("Control attribute %Qlv cannot %v",
        ControlAttribute,
        whatsWrong));
}

void TLegacyTableConsumer::ThrowError(const Stroka& message) const
{
    THROW_ERROR_EXCEPTION(message)
        << TErrorAttribute("table_index", CurrentTableIndex)
        << TErrorAttribute("row_index", Writer->GetRowCount());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
