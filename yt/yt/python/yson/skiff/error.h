#include <yt/yt/core/misc/error.h>

#include "converter_common.h"

#include <Objects.hxx> // pycxx

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

Py::Exception CreateSkiffError(
    const TString& message,
    const TError& error,
    TSkiffRowContext* rowContext = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
