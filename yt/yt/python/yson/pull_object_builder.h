#pragma once

#include <yt/yt/python/common/cache.h>

#include <yt/yt/core/misc/optional.h>
#include <yt/yt/core/misc/ref.h>

#include <yt/yt/core/yson/consumer.h>
#include <yt/yt/core/yson/pull_parser.h>

#include <yt/yt/core/ytree/public.h>

#include <Objects.hxx> // pycxx

namespace NYT::NPython {

using NPython::PyObjectPtr;

////////////////////////////////////////////////////////////////////////////////

class TPullObjectBuilder
{
public:
    TPullObjectBuilder(
        NYson::TYsonPullParser* parser,
        bool alwaysCreateAttributes,
        const std::optional<TString>& encoding);
    
    // Parse object and move cursor.
    PyObjectPtr ParseObject(bool hasAttributes = false);
    
    // Parse map, cursor must point to the first key.
    PyObjectPtr ParseMap(NYson::EYsonItemType endType, bool hasAttributes = false);

private:
    NYT::NYson::TYsonPullParserCursor Cursor_;
    Py::Callable YsonMap;
    Py::Callable YsonList;
    Py::Callable YsonString;
#if PY_MAJOR_VERSION >= 3
    Py::Callable YsonUnicode;
    std::optional<Py::Callable> YsonStringProxy;
#endif
    Py::Callable YsonInt64;
    Py::Callable YsonUint64;
    Py::Callable YsonDouble;
    Py::Callable YsonBoolean;
    Py::Callable YsonEntity;

    bool AlwaysCreateAttributes_;
    std::optional<TString> Encoding_;

    NPython::TPythonStringCache KeyCache_;
    PyObjectPtr Tuple0_;
    PyObjectPtr Tuple1_;

    // Parse list of objects, cursor must point to the first element.
    PyObjectPtr ParseList(bool hasAttributes = false);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
