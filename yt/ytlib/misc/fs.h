#pragma once

#include "common.h"

namespace NYT {
namespace NFS {

////////////////////////////////////////////////////////////////////////////////

const char* const TempFileSuffix = "~";

bool Remove(const char* name);
bool Rename(const char* oldName, const char* newName);

Stroka GetFileName(Stroka filePath);
Stroka GetFileExtension(Stroka fileName);
Stroka GetFileNameWithoutExtension(Stroka fileName);
void CleanTempFiles(Stroka location);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFS
} // namespace NYT
