#pragma once

#include "public.h"
#include <ytlib/yson/public.h>

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

};

////////////////////////////////////////////////////////////////////////////////

void ProduceYson(ISyncReaderPtr Reader, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
