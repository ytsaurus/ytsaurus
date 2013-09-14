#pragma once

#include "public.h"

#include <ytlib/yson/public.h>
#include <ytlib/misc/nullable.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TTableProducer
{
public:
    TTableProducer(ISyncReaderPtr reader, NYson::IYsonConsumer* consumer, int tableIndex = 0);
    bool ProduceRow();

private:
    ISyncReaderPtr Reader;
    NYson::IYsonConsumer* Consumer;

    int TableIndex;

};

////////////////////////////////////////////////////////////////////////////////

void ProduceRow(NYson::IYsonConsumer* consumer, const TRow& row);
void ProduceTableSwitch(NYson::IYsonConsumer* consumer, int tableIndex);

////////////////////////////////////////////////////////////////////////////////

void ProduceYson(ISyncReaderPtr Reader, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
