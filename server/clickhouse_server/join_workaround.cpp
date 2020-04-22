#include "join_workaround.h"

#include "helpers.h"

#include <yt/core/ytree/node.h>
#include <yt/core/ytree/convert.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Interpreters/IdentifierSemantic.h>

namespace NYT::NClickHouseServer {

using namespace NYson;
using namespace NYTree;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

INodePtr DumpMembership(const DB::IAST& ast, const NLogging::TLogger& logger)
{
    const auto& Logger = logger;

    if (const auto* identifier = ast.as<DB::ASTIdentifier>()) {
        auto membership = DB::IdentifierSemantic::getMembership(*identifier);
        YT_LOG_TRACE("Dumping membership for identifier (Identifier: %v, Membership: %v)", ast, membership);
        return ConvertToNode(membership);
    }

    if (auto* tableExpression = ast.as<DB::ASTTableExpression>()) {
        return ConvertToNode(TYsonString("#"));
    }

    std::vector<INodePtr> result;
    for (const auto& child : ast.children) {
        result.emplace_back(DumpMembership(*child, logger));
    }
    return ConvertToNode(result);
}

using TModificationVector = std::vector<std::pair<DB::ASTIdentifier*, ui64>>;

// Note that we either apply whole hint, or do not apply it at all. Thus, we have to postpone application of the
// modifications till the end of the recursive AST visit.
void ApplyMembership(DB::IAST& ast, const INodePtr& hintNode, TString path, TModificationVector& modifications)
{
    auto validateType = [&] (ENodeType expected) {
        if (hintNode->GetType() != expected) {
            THROW_ERROR_EXCEPTION("Wrong hint node type at '%v': expected = %Qlv, actual = %Qlv",
                path,
                expected,
                hintNode->GetType());
        }
    };

    if (auto* identifier = ast.as<DB::ASTIdentifier>()) {
        validateType(ENodeType::Uint64);
        modifications.emplace_back(identifier, hintNode->AsUint64()->GetValue());
        return;
    }

    if (auto* tableExpression = ast.as<DB::ASTTableExpression>()) {
        validateType(ENodeType::Entity);
        return;
    }

    validateType(ENodeType::List);
    const auto& hintListNode = hintNode->AsList();
    if (static_cast<size_t>(hintListNode->GetChildCount()) != ast.children.size()) {
        THROW_ERROR_EXCEPTION("Child count mismatch at '%v': expected = %v, actual = %v",
            path,
            ast.children.size(),
            hintListNode->GetChildCount());
    }
    for (size_t index = 0; index < ast.children.size(); ++index) {
        ApplyMembership(*ast.children[index], hintListNode->GetChild(index), path + "/" + ToString(index), modifications);
    }
}

void ApplyMembershipHint(DB::IAST& ast, const TYsonString& hint, const TLogger& logger)
{
    const auto& Logger = logger;

    YT_LOG_TRACE("Applying membership hint to AST (AST: %v, Hint: %v)", ast, hint);
    auto hintNode = ConvertToNode(hint);
    TModificationVector modifications;
    ApplyMembership(ast, hintNode, "" /* path */, modifications);
    for (auto& [identifier, membership] : modifications) {
        YT_LOG_TRACE("Applying membership for identifier (Identifier: %v, Membership: %v)",
            static_cast<const DB::IAST&>(*identifier),
            membership);
        DB::IdentifierSemantic::setMembership(*identifier, membership);
    }
    YT_LOG_DEBUG("AST hint successfully applied");
}

TYsonString DumpMembershipHint(const DB::IAST& ast, const TLogger& logger)
{
    const auto& Logger = logger;
    auto hintNode = DumpMembership(ast, logger);
    auto hint = ConvertToYsonString(hintNode, EYsonFormat::Text);
    YT_LOG_DEBUG("Dumped membership hint for AST");
    YT_LOG_TRACE("Dumped membership hint for AST (AST: %v, Hint: %v)", ast, hint);
    return hint;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
