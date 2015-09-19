#pragma once

#include "yt_udf_types.h"

#include <stdlib.h>

char* AllocatePermanentBytes(TExecutionContext* context, size_t size);

char* AllocateBytes(TExecutionContext* context, size_t size);

void ThrowException(const char* error);
