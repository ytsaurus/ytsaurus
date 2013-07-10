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
    TTableProducer(ISyncReaderPtr reader, NYson::IYsonConsumer* consumer);
    bool ProduceRow();

private:
    ISyncReaderPtr Reader;
    NYson::IYsonConsumer* Consumer;

    TNullable<int> TableIndex;

};

////////////////////////////////////////////////////////////////////////////////

void ProduceYson(ISyncReaderPtr Reader, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
