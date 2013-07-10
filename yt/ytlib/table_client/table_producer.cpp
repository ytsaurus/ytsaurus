#include "stdafx.h"
#include "table_producer.h"
#include "sync_reader.h"

#include <ytlib/yson/consumer.h>
#include <ytlib/ytree/yson_string.h>
#include <ytlib/misc/foreach.h>

namespace NYT {
namespace NTableClient {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TTableProducer::TTableProducer(
    ISyncReaderPtr reader,
    IYsonConsumer* consumer)
    : Reader(reader)
    , Consumer(consumer)
    , TableIndex(Null)
{ }

bool TTableProducer::ProduceRow()
{
    static Stroka tableIndexKey = FormatEnum(EControlAttribute(EControlAttribute::TableIndex));

    auto row = Reader->GetRow();
    if (!row) {
        return false;
    }

    const auto& tableIndex = Reader->GetTableIndex();

    if (tableIndex != TableIndex) {
        TableIndex = tableIndex;
        YCHECK(tableIndex);

        Consumer->OnListItem();
        Consumer->OnBeginAttributes();
        Consumer->OnKeyedItem(tableIndexKey);
        Consumer->OnIntegerScalar(*TableIndex);
        Consumer->OnEndAttributes();
        Consumer->OnEntity();
    }

    Consumer->OnListItem();
    Consumer->OnBeginMap();
    FOREACH (auto& pair, *row) {
        Consumer->OnKeyedItem(pair.first);
        Consumer->OnRaw(pair.second, EYsonType::Node);
    }
    Consumer->OnEndMap();

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void ProduceYson(ISyncReaderPtr reader, NYson::IYsonConsumer* consumer)
{
    TTableProducer producer(reader, consumer);
    while (producer.ProduceRow());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
