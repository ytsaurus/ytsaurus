#pragma once

#include "public.h"
#include "schema.h"

#include <yt/yt/python/common/helpers.h>
#include <yt/yt/python/common/stream.h>

#include <yt/yt/library/skiff_ext/parser.h>

#include <CXX/Extensions.hxx> // pycxx
#include <CXX/Objects.hxx> // pycxx

#include <util/generic/string.h>
#include <util/generic/hash.h>

#include <vector>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

template <class TConsumer>
std::unique_ptr<NSkiffExt::TSkiffMultiTableParser<TConsumer>> CreateSkiffMultiTableParser(
    TConsumer* consumer,
    const std::vector<Py::PythonClassObject<TSkiffSchemaPython>>& pythonSkiffSchemaList,
    const TString& rangeIndexColumnName,
    const TString& rowIndexColumnName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython

#define PARSER_HELPERS_INL_H_
#include "parser_helpers-inl.h"
#undef PARSER_HELPERS_INL_H_
