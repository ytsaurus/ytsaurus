#pragma once

#include "common.h"
#include "sync_reader.h"

#include <ytlib/ytree/yson_writer.h>
#include <util/stream/output.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////

class TYsonTableInput
{
public:
    TYsonTableInput(
        ISyncTableReader::TPtr reader,
        TOutputStream* outputStream,
        NYTree::EYsonFormat format);

    bool ReadRow();

private:
    ISyncTableReader::TPtr Reader;
    NYTree::TYsonWriter YsonWriter;

};

////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
