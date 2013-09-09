#pragma once

#include <core/misc/common.h>
#include <core/logging/log.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger FileReaderLogger;
extern NLog::TLogger FileWriterLogger;

extern int FormatVersion;

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT

