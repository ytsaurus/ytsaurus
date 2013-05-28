#pragma once

#include "public.h"

#include <ytlib/yson/public.h>
#include <ytlib/ytree/public.h>

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

void ProduceRow(NYson::IYsonConsumer* consumer, const TRow& row, const NYTree::TYsonString& attributes);

////////////////////////////////////////////////////////////////////////////////

void ProduceYson(ISyncReaderPtr Reader, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
