#include "stdafx.h"
#include "value_consumer.h"
#include "key.h"

namespace NYT {
namespace NTableClient {

using namespace NYT;

////////////////////////////////////////////////////////////////////////////////

TValueConsumer::TValueConsumer(TOutputStream* output)
    : TYsonWriter(output)
    , NewValue(false)
    , Key(NULL)
    , KeyIndex(-1)
{ }

void TValueConsumer::OnNewValue(TKey* key, int keyIndex)
{
    NewValue = true;
    Key = key;
    KeyIndex = keyIndex;
}

void TValueConsumer::OnStringScalar(const TStringBuf& value, bool hasAttributes)
{
    if (NewValue) {
        Key->AddString(KeyIndex, value);
        NewValue = false;
    }

    TYsonWriter::OnStringScalar(value, hasAttributes);
}

void TValueConsumer::OnIntegerScalar(i64 value, bool hasAttributes)
{
    if (NewValue) {
        Key->AddInteger(KeyIndex, value);
        NewValue = false;
    }

    TYsonWriter::OnIntegerScalar(value, hasAttributes);
}

void TValueConsumer::OnDoubleScalar(double value, bool hasAttributes)
{
    if (NewValue) {
        Key->AddDouble(KeyIndex, value);
        NewValue = false;
    }

    TYsonWriter::OnDoubleScalar(value, hasAttributes);
}

void TValueConsumer::OnEntity(bool hasAttributes)
{
    if (NewValue) {
        Key->AddComposite(KeyIndex);
        NewValue = false;
    }

    TYsonWriter::OnEntity(hasAttributes);
}

void TValueConsumer::OnBeginList()
{
    if (NewValue) {
        Key->AddComposite(KeyIndex);
        NewValue = false;
    }

    TYsonWriter::OnBeginList();
}

void TValueConsumer::OnBeginMap()
{
    if (NewValue) {
        Key->AddComposite(KeyIndex);
        NewValue = false;
    }

    TYsonWriter::OnBeginMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
