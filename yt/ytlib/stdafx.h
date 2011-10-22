#pragma once

#include <algorithm>

#include <string>
// TODO: try to get rid of this
using std::string; // hack for guid.h to work

#include <util/system/atomic.h>
#include <util/system/defaults.h>
#include <util/system/mutex.h>
#include <util/system/event.h>
#include <util/system/thread.h>
#include <util/system/file.h>
#include <util/system/hostname.h>
#include <util/system/yield.h>
#include <util/system/atexit.h>
#include <util/system/spinlock.h>

#include <util/charset/wide.h>

#include <util/thread/lfqueue.h>

#include <util/memory/tempbuf.h>

#include <util/generic/list.h>
#include <util/generic/deque.h>
#include <util/generic/utility.h>
#include <util/generic/stroka.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/singleton.h>
#include <util/generic/typehelpers.h>
#include <util/generic/yexception.h>
#include <util/generic/pair.h>
#include <util/generic/algorithm.h>

#include <util/datetime/base.h>
#include <util/datetime/cputimer.h>

#include <util/string/printf.h>
#include <util/string/cast.h>

#include <util/random/random.h>

#include <util/stream/str.h>
#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/stream/file.h>

#include <util/folder/filelist.h>
#include <util/folder/dirut.h>

#include <util/config/last_getopt.h>

#include <util/server/http.h>
#include <util/autoarray.h>
#include <util/ysaveload.h>
#include <util/str_stl.h>

#include <quality/NetLiba/UdpHttp.h>
#include <quality/NetLiba/UdpAddress.h>

#include <library/json/json_writer.h>

#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>
#include <contrib/libs/protobuf/io/coded_stream.h>
#include <contrib/libs/protobuf/stubs/once.h>
#include <contrib/libs/protobuf/descriptor.h>
#include <contrib/libs/protobuf/reflection_ops.h>
#include <contrib/libs/protobuf/repeated_field.h>
#include <contrib/libs/protobuf/message.h>
#include <contrib/libs/protobuf/message_lite.h>

#ifdef _MSC_VER
    // For protobuf-generated files:
    // C4125: decimal digit terminates octal escape sequence
    #pragma warning (disable : 4125)
#endif

