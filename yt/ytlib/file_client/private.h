#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/logging/log.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger FileReaderLogger;
extern NLog::TLogger FileWriterLogger;

extern int FormatVersion;

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT

