#include <ytlib/query_client/parser.hpp>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TLexer
{
public:
    TLexer(TQueryContext* context, const Stroka& source);

    TParser::token_type GetNextToken(
        TParser::semantic_type* yyval,
        TParser::location_type* yyloc);

private:
    void Initialize(const char* begin, const char* end);

private:
    TQueryContext* Context_;

    // Ragel state variables.
    // See Ragel User Manual for host interface specification.
    const char* p;
    const char* pe;
    const char* ts;
    const char* te;
    const char* eof;
    int cs;
    int act;

    // Saves embedded chunk boundaries and embedding depth.
    const char* rs;
    const char* re;
    int rd;

    // Saves beginning-of-string boundary to compute locations.
    const char* s;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

