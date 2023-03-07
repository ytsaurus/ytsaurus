#pragma once

#include "public.h"
#include "schema.h"

#include <yt/python/common/helpers.h>
#include <yt/python/common/stream.h>

#include <yt/library/skiff/parser.h>
#include <yt/library/skiff/skiff_schema.h>

#include <Extensions.hxx> // pycxx
#include <Objects.hxx> // pycxx

#include <util/generic/string.h>
#include <util/generic/hash.h>

#include <vector>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

template <class TConsumer>
std::unique_ptr<NSkiff::TSkiffMultiTableParser<TConsumer>> CreateSkiffMultiTableParser(
    TConsumer* consumer,
    const std::vector<Py::PythonClassObject<TSkiffSchemaPython>>& pythonSkiffSchemaList,
    const TString& rangeIndexColumnName,
    const TString& rowIndexColumnName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython

#define PARSER_HELPERS_INL_H_
#include "parser_helpers-inl.h"
#undef PARSER_HELPERS_INL_H_
