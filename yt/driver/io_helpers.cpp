#include "io_helpers.h"

#include <core/misc/tclap_helpers.h>

namespace NYT {
namespace NYPath {

std::istringstream& operator >> (std::istringstream& input, NYPath::TRichYPath& path)
{
    auto str = ReadAll(input);
    path = TRichYPath::Parse(str);
    return input;
}

} // namespace NYPath
} // namespace NYT
