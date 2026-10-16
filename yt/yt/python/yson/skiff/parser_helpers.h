#pragma once

#include "public.h"
#include "multi_table_parser.h"
#include "schema.h"

#include <yt/yt/python/common/helpers.h>
#include <yt/yt/python/common/stream.h>

#include <CXX/Extensions.hxx> // pycxx
#include <CXX/Objects.hxx> // pycxx

#include <util/generic/string.h>
#include <util/generic/hash.h>

#include <vector>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

template <class TConsumer>
std::unique_ptr<TSkiffMultiTableParser<TConsumer>> CreateSkiffMultiTableParser(
    TConsumer* consumer,
    const std::vector<Py::PythonClassObject<TSkiffSchemaPython>>& pythonSkiffSchemaList,
    const std::string& rangeIndexColumnName,
    const std::string& rowIndexColumnName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython

#define PARSER_HELPERS_INL_H_
#include "parser_helpers-inl.h"
#undef PARSER_HELPERS_INL_H_
