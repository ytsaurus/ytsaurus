#pragma once

#include "public.h"

#include <core/yson/public.h>
#include <core/misc/nullable.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TTableProducer
{
public:
    TTableProducer(
        ISyncReaderPtr reader,
        NYson::IYsonConsumer* consumer,
        bool enableTableSwitch = true,
        int tableIndex = -1);

    bool ProduceRow();

private:
    ISyncReaderPtr Reader;
    NYson::IYsonConsumer* Consumer;

    bool EnableTableSwitch;
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
