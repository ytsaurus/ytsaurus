#include "stdafx.h"
#include "yson_table_input.h"

#include <ytlib/misc/foreach.h>

namespace NYT {
namespace NTableClient {

using namespace NYTree;

////////////////////////////////////////////////////////////////////

TYsonTableInput::TYsonTableInput(
    ISyncTableReader::TPtr reader, 
    TOutputStream* outputStream,
    EYsonFormat format)
    : Reader(reader)
    , YsonWriter(outputStream, format, EYsonType::ListFragment)
{
    YASSERT(reader);
    YASSERT(outputStream);

    Reader->Open();
}

bool TYsonTableInput::ReadRow()
{
    if (!Reader->IsValid())
        return false;

    YsonWriter.OnListItem();
    YsonWriter.OnBeginMap();
    FOREACH (auto& pair, Reader->GetRow()) {
        YsonWriter.OnKeyedItem(pair.first);
        YsonWriter.OnStringScalar(pair.second.ToString());
    }
    YsonWriter.OnEndMap();

    Reader->NextRow();
    return true;
}

////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
