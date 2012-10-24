#include "stdafx.h"
#include "attribute_provider.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TAttributeFilter TAttributeFilter::All(EAttributeFilterMode::All, std::vector<Stroka>());
TAttributeFilter TAttributeFilter::None(EAttributeFilterMode::None, std::vector<Stroka>());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
