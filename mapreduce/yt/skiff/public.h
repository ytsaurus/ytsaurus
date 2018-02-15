#pragma once

#include <util/generic/ptr.h>

namespace NSkiff {

////////////////////////////////////////////////////////////////////////////////

class TSkiffSchema;
using TSkiffSchemaPtr = TIntrusivePtr<TSkiffSchema>;

class TSkiffValidator;

class TUncheckedSkiffParser;
class TCheckedSkiffParser;

class TUncheckedSkiffWriter;
class TCheckedSkiffWriter;

#ifdef DEBUG
using TCheckedInDebugSkiffParser = TCheckedSkiffParser;
using TCheckedInDebugSkiffWriter = TCheckedSkiffWriter;
#else
using TCheckedInDebugSkiffParser = TUncheckedSkiffParser;
using TCheckedInDebugSkiffWriter = TUncheckedSkiffWriter;
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkiff
