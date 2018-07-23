#pragma once

#include "serialize.h"

#include <yt/python/common/error.h>

#include <yt/core/ytree/convert.h>

namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

Py::Exception CreateYsonError(const TString& message, const TError& error);
Py::Exception CreateYsonError(const TString& message, TContext* context);

#define CATCH_AND_CREATE_YSON_ERROR(message) \
    catch (const NYT::TErrorException& error) { \
        throw CreateYsonError(message, error.Error()); \
    } catch (const std::exception& ex) { \
        if (PyErr_ExceptionMatches(PyExc_BaseException)) { \
            throw; \
        } else { \
            throw CreateYsonError(message, TError(ex)); \
        } \
    }

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
