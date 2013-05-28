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
{ }

bool TTableProducer::ProduceRow()
{
    auto row = Reader->GetRow();
    if (!row) {
        return false;
    }

    NTableClient::ProduceRow(Consumer, *row, Reader->GetRowAttributes());

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void ProduceRow(IYsonConsumer* consumer, const TRow& row, const TYsonString& attributes)
{
    consumer->OnListItem();

    if (!attributes.Data().empty()) {
        consumer->OnBeginAttributes();
        consumer->OnRaw(attributes.Data(), EYsonType::MapFragment);
        consumer->OnEndAttributes();
    }

    consumer->OnBeginMap();
    FOREACH (const auto& pair, row) {
        consumer->OnKeyedItem(pair.first);
        consumer->OnRaw(pair.second, EYsonType::Node);
    }
    consumer->OnEndMap();
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
