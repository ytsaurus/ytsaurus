#include <Parsers/ASTFunctionWithKeyValueArguments.h>

#include <DBPoco/String.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>

namespace DB
{

String ASTPair::getID(char) const
{
    return "pair";
}


ASTPtr ASTPair::clone() const
{
    auto res = std::make_shared<ASTPair>(*this);
    res->children.clear();
    res->set(res->second, second->clone());
    return res;
}


void ASTPair::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << DBPoco::toUpper(first) << " " << (settings.hilite ? hilite_none : "");

    if (second_with_brackets)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "(";

    if (!settings.show_secrets && (first == "password"))
    {
        /// Hide password in the definition of a dictionary:
        /// SOURCE(CLICKHOUSE(host 'example01-01-1' port 9000 user 'default' password '[HIDDEN]' db 'default' table 'ids'))
        settings.ostr << "'[HIDDEN]'";
    }
    else
    {
        second->formatImpl(settings, state, frame);
    }

    if (second_with_brackets)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << ")";

    settings.ostr << (settings.hilite ? hilite_none : "");
}


bool ASTPair::hasSecretParts() const
{
    return first == "password";
}


void ASTPair::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(first.size());
    hash_state.update(first);
    hash_state.update(second_with_brackets);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}


String ASTFunctionWithKeyValueArguments::getID(char delim) const
{
    return "FunctionWithKeyValueArguments " + (delim + name);
}


ASTPtr ASTFunctionWithKeyValueArguments::clone() const
{
    auto res = std::make_shared<ASTFunctionWithKeyValueArguments>(*this);
    res->children.clear();

    if (elements)
    {
        res->elements = elements->clone();
        res->children.push_back(res->elements);
    }

    return res;
}


void ASTFunctionWithKeyValueArguments::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << DBPoco::toUpper(name) << (settings.hilite ? hilite_none : "") << (has_brackets ? "(" : "");
    elements->formatImpl(settings, state, frame);
    settings.ostr << (has_brackets ? ")" : "");
    settings.ostr << (settings.hilite ? hilite_none : "");
}


void ASTFunctionWithKeyValueArguments::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(name.size());
    hash_state.update(name);
    hash_state.update(has_brackets);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

}
