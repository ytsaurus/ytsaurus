#include <yt/python/common/error.h>

#include <yt/python/yson/serialize.h>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

Py::Exception CreateYtError(const TString& message, const TError& error);

#define CATCH_AND_CREATE_YT_ERROR(message) \
    catch (const NYT::TErrorException& errorEx) { \
        throw CreateYtError(message, errorEx.Error()); \
    } catch (const std::exception& ex) { \
        if (PyErr_ExceptionMatches(PyExc_BaseException)) { \
            throw; \
        } else { \
            throw CreateYtError(message, TError(ex)); \
        } \
    }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
