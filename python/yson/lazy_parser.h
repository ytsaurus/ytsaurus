#pragma once

#include <yt/core/misc/nullable.h>
#include <yt/core/yson/public.h>

#include <Objects.hxx> // pycxx

#include <util/stream/input.h>
#include <util/generic/string.h>

namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

Py::Object ParseLazyYson(
    IInputStream* inputStream,
    const TNullable<TString>& encoding,
    bool alwaysCreateAttributes,
    NYson::EYsonType ysonType);

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
