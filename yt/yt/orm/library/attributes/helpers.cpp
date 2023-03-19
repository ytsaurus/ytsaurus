#include "helpers.h"

#include <yt/yt/core/ypath/token.h>
#include <yt/yt/core/ypath/tokenizer.h>

namespace NYT::NOrm::NAttributes {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

void ValidateAttributePath(const NYPath::TYPath& path)
{
    TTokenizer tokenizer(path);
    while (tokenizer.Advance() != ETokenType::EndOfStream) {
        tokenizer.Expect(NYPath::ETokenType::Slash);

        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
