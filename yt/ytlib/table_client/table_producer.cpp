#include "stdafx.h"
#include "table_producer.h"
#include "sync_reader.h"

#include <ytlib/ytree/yson_consumer.h>
#include <ytlib/ytree/yson_string.h>
#include <ytlib/misc/foreach.h>

namespace NYT {
namespace NTableClient {

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
    /*const auto& attributes = Reader->GetRowAttributes();
    if (!attributes.empty())
        Consumer->OnRaw(Reader->GetRowAttributes(), EYsonType::Node);
    */

    auto row = Reader->GetRow();
    if (!row) {
        return false;
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

void ProduceYson(ISyncReaderPtr reader, NYTree::IYsonConsumer* consumer)
{
    TTableProducer producer(reader, consumer);
    while (producer.ProduceRow());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
