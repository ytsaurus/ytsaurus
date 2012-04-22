#pragma once

#include "public.h"
#include <ytlib/ytree/public.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TTableProducer
{
public:
    TTableProducer(ISyncReaderPtr reader, NYTree::IYsonConsumer* consumer);
    bool ProduceRow();

private:
    ISyncReaderPtr Reader;
    NYTree::IYsonConsumer* Consumer;

};

////////////////////////////////////////////////////////////////////////////////

void ProduceYson(ISyncReaderPtr Reader, NYTree::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
