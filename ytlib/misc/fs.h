#pragma once

#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

const char* const TempFileSuffix = "~";

Stroka GetFileExtension(Stroka fileName);
Stroka GetFileNameWithoutExtension(Stroka fileName);
void CleanTempFiles(Stroka location);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT


