#pragma once

#include "yt_udf_types.h"

#include <ytlib/query_client/function_context.h>

#include <stdlib.h>

extern "C" char* AllocatePermanentBytes(TExecutionContext* context, size_t size);

extern "C" char* AllocateBytes(TExecutionContext* context, size_t size);

extern "C" void ThrowException(const char* error);

