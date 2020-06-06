#pragma once

#include "public.h"

#include <yt/client/table_client/public.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

IRowStreamFormatterPtr CreateWireRowStreamFormatter(NTableClient::TNameTablePtr nameTable);
IRowStreamParserPtr CreateWireRowStreamParser(NTableClient::TNameTablePtr nameTable);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
