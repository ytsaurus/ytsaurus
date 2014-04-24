#pragma once

#include <core/misc/intrusive_ptr.h>

#include <vector>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

class TLogConfig;
typedef TIntrusivePtr<TLogConfig> TLogConfigPtr;

struct ILogWriter;
typedef TIntrusivePtr<ILogWriter> ILogWriterPtr;
typedef std::vector<ILogWriterPtr> ILogWriters;

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
