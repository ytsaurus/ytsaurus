#pragma once

#include "options.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TYdbPayloadSplitter CreateWholeYdbPayloadSplitter();
TYdbPayloadSplitter CreateFramingYdbPayloadSplitter(EFramingFormat framingFormat);
TYdbPayloadSplitter CreateMultiLineYdbPayloadSplitter(char rowDelimiter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
