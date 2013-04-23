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

    Consumer->OnListItem();

    const auto& attributes = Reader->GetRowAttributes();
    if (!attributes.Data().empty()) {
        Consumer->OnBeginAttributes();
        Consumer->OnRaw(attributes.Data(), EYsonType::MapFragment);
        Consumer->OnEndAttributes();
    }

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
