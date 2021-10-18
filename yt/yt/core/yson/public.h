#pragma once

#include <yt/yt/core/misc/public.h>

#include <library/cpp/yson/public.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

using ::NYson::EYsonFormat;
using ::NYson::EYsonType;

enum class ETokenType;

class TYsonString;
class TYsonStringBuf;

class TYsonProducer;

class TYsonInput;
class TYsonOutput;

class TUncheckedYsonTokenWriter;
class TCheckedYsonTokenWriter;

#ifdef NDEBUG
using TCheckedInDebugYsonTokenWriter = TUncheckedYsonTokenWriter;
#else
using TCheckedInDebugYsonTokenWriter = TCheckedYsonTokenWriter;
#endif

class TTokenizer;

class TProtobufMessageType;

struct IYsonConsumer;
struct IFlushableYsonConsumer;
struct IAsyncYsonConsumer;

enum class EYsonItemType : ui8;
class TYsonItem;
class TYsonPullParser;
class TYsonPullParserCursor;

class TForwardingYsonConsumer;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
