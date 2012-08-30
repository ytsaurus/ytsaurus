#include "stdafx.h"
#include "tclap_helpers.h"

#include <ytlib/misc/error.h>

#include <ytlib/ytree/tokenizer.h>
#include <ytlib/ytree/token.h>
#include <ytlib/ytree/attribute_helpers.h>

#include <iterator>

namespace NYT
{

////////////////////////////////////////////////////////////////////////////////

Stroka ReadAll(std::istringstream& input)
{
    Stroka result(input.str());
    input.ignore(std::numeric_limits<std::streamsize>::max());
    return result;
}

std::istringstream& operator >> (std::istringstream& input, TGuid& guid)
{
    auto str = ReadAll(input);
    guid = TGuid::FromString(str);
    return input;
}

namespace NYTree {

std::istringstream& operator>>(std::istringstream& input, TRichYPath& path)
{
    auto str = ReadAll(input);
    if (!str.empty() && str[0] == TokenTypeToChar(ETokenType::LeftAngle)) {
        // Look for the matching right angle.
        TTokenizer tokenizer(str);
        int depth = 0;
        int attrStartPosition = -1;
        int attrEndPosition = -1;
        int pathStartPosition = -1;
        while (true) {
            int positionBefore = str.length() - tokenizer.GetCurrentSuffix().length();
            if (!tokenizer.ParseNext()) {
                THROW_ERROR_EXCEPTION("Unmatched '<' in YPath");
            }
            int positionAfter = str.length() - tokenizer.GetCurrentSuffix().length();

            switch (tokenizer.CurrentToken().GetType()) {
                case ETokenType::LeftAngle:
                    ++depth;
                    break;
                case ETokenType::RightAngle:
                    --depth;
                    break;
            }

            if (attrStartPosition < 0 && depth == 1) {
                attrStartPosition = positionAfter;
            }

            if (attrEndPosition < 0 && depth == 0) {
                attrEndPosition = positionBefore;
                pathStartPosition = positionAfter;
                break;
            }
        }

        TYsonString attrYson(
            str.substr(attrStartPosition, attrEndPosition - attrStartPosition),
            EYsonType::MapFragment);

        path.SetPath(str.substr(pathStartPosition));
        path.Attributes().Clear();
        path.Attributes().MergeFrom(*ConvertToAttributes(attrYson));
    } else {
        path.SetPath(str);
        path.Attributes().Clear();
    }
    return input;
}

} // namespace NYTree

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

