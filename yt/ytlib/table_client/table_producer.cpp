#include "stdafx.h"
#include "table_producer.h"
#include "sync_reader.h"

#include <ytlib/ytree/yson_consumer.h>
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
    if (!Reader->IsValid())
        return false;

    /*const auto& attributes = Reader->GetRowAttributes();
    if (!attributes.empty())
        Consumer->OnRaw(Reader->GetRowAttributes(), EYsonType::Node);
    */

    Consumer->OnListItem();
    Consumer->OnBeginMap();
    FOREACH (auto& pair, Reader->GetRow()) {
        Consumer->OnKeyedItem(pair.first);
        Consumer->OnRaw(pair.second, EYsonType::Node);
    }
    Consumer->OnEndMap();

    Reader->NextRow();
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
