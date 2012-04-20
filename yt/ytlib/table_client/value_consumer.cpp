#include "stdafx.h"
#include "value_consumer.h"
#include "key.h"

namespace NYT {
namespace NTableClient {

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

void TValueConsumer::OnStringScalar(const TStringBuf& value)
{
    if (NewValue) {
        Key->AddValue(KeyIndex, value);
        NewValue = false;
    }

    TYsonWriter::OnStringScalar(value);
}

void TValueConsumer::OnIntegerScalar(i64 value)
{
    if (NewValue) {
        Key->AddValue(KeyIndex, value);
        NewValue = false;
    }

    TYsonWriter::OnIntegerScalar(value);
}

void TValueConsumer::OnDoubleScalar(double value)
{
    if (NewValue) {
        Key->AddValue(KeyIndex, value);
        NewValue = false;
    }

    TYsonWriter::OnDoubleScalar(value);
}

void TValueConsumer::OnEntity()
{
    if (NewValue) {
        Key->AddComposite(KeyIndex);
        NewValue = false;
    }

    TYsonWriter::OnEntity();
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
