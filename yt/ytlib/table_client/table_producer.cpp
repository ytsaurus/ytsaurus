#include "stdafx.h"
#include "table_producer.h"
#include "sync_reader.h"

#include <core/misc/string.h>

#include <core/yson/consumer.h>

#include <core/ytree/yson_string.h>

namespace NYT {
namespace NTableClient {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TTableProducer::TTableProducer(
    ISyncReaderPtr reader,
    IYsonConsumer* consumer,
    bool enableTableSwitch,
    int tableIndex)
    : Reader(reader)
    , Consumer(consumer)
    , EnableTableSwitch(enableTableSwitch)
    , TableIndex(tableIndex)
{ }

bool TTableProducer::ProduceRow()
{
    auto row = Reader->GetRow();
    if (!row) {
        return false;
    }

    int tableIndex = Reader->GetTableIndex();

    if (tableIndex != TableIndex) {
        TableIndex = tableIndex;
        if (EnableTableSwitch) {
            NTableClient::ProduceTableSwitch(Consumer, TableIndex);
        }
    }

    NTableClient::ProduceRow(Consumer, *row);

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void ProduceRow(IYsonConsumer* consumer, const TRow& row)
{
    consumer->OnListItem();

    consumer->OnBeginMap();
    for (const auto& pair : row) {
        consumer->OnKeyedItem(pair.first);
        consumer->OnRaw(pair.second, EYsonType::Node);
    }
    consumer->OnEndMap();
}

void ProduceTableSwitch(IYsonConsumer* consumer, int tableIndex)
{
    static Stroka tableIndexKey = FormatEnum(EControlAttribute(EControlAttribute::TableIndex));

    consumer->OnListItem();
    consumer->OnBeginAttributes();
    consumer->OnKeyedItem(tableIndexKey);
    consumer->OnInt64Scalar(tableIndex);
    consumer->OnEndAttributes();
    consumer->OnEntity();
}

void ProduceYson(ISyncReaderPtr reader, NYson::IYsonConsumer* consumer)
{
    TTableProducer producer(reader, consumer);
    while (producer.ProduceRow());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
