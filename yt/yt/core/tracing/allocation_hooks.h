#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void* CreateAllocationTagsData();
void* CopyAllocationTagsData(void* ptr);
void DestroyAllocationTagsData(void* ptr);
const std::vector<std::pair<TString, TString>>& ReadAllocationTagsData(void* ptr);
void StartAllocationTagsCleanupThread(TDuration cleanupInterval);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
