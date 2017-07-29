#pragma once

#include "helpers.h"
#include "public.h"
#include "stream.h"
#include "object_builder.h"
#include "lazy_dict.h"

#include <yt/core/yson/lexer_detail.h>

#include <yt/core/ytree/convert.h>

#include <contrib/libs/pycxx/Extensions.hxx>
#include <contrib/libs/pycxx/Objects.hxx>

#include <Python.h>

namespace NYT {
namespace NPython {

///////////////////////////////////////////////////////////////////////////////

PyObject* ParseLazyDict(
    TInputStream* stream,
    NYson::EYsonType parsingMode,
    const TNullable<TString>& encoding,
    bool alwaysCreateAttributes,
    NPython::TPythonStringCache* keyCacher);

///////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
