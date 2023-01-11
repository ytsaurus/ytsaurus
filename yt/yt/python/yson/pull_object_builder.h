#pragma once

#include <yt/yt/python/common/cache.h>

#include <yt/yt/core/yson/consumer.h>
#include <yt/yt/core/yson/pull_parser.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/memory/ref.h>

#include <CXX/Objects.hxx> // pycxx

#include <optional>

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

    PyObjectPtr ParseMapLazy(NYson::EYsonItemType endType);

    PyObjectPtr ParseObjectLazy(bool hasAttributes = false);

private:
    NYT::NYson::TYsonPullParserCursor Cursor_;

    bool AlwaysCreateAttributes_;
    std::optional<TString> Encoding_;

    NPython::TPythonStringCache KeyCache_;
    PyObjectPtr Tuple0_;
    PyObjectPtr Tuple1_;

    // Encoding and AlwaysCreateAttributes for Python lazy_dict.
    Py::Object LazyMapParserParams_;

    // Parse list of objects, cursor must point to the first element.
    PyObjectPtr ParseList(bool hasAttributes = false);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
