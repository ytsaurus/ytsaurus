//#include "portal_tree.h"
//
//#include <yt/core/ypath/tokenizer.h>
//
//namespace NYT::NCypressServer {
//
//using namespace NConcurrency;
//using namespace NYPath;
//
//////////////////////////////////////////////////////////////////////////////////
//
//TNodeId TPortalTree::ParseRootId(
//    TNodeId rootId,
//    TTokenizer* tokenizer)
//{
//    switch (tokenizer.Advance()) {
//        case NYPath::ETokenType::EndOfStream:
//            THROW_ERROR_EXCEPTION("YPath cannot be empty");
//
//        case NYPath::ETokenType::Slash:
//            return rootId;
//
//        case NYPath::ETokenType::Literal: {
//            const auto& token = tokenizer.GetToken();
//            if (!token.StartsWith(ObjectIdPathPrefix)) {
//                tokenizer.ThrowUnexpected();
//            }
//
//            TStringBuf objectIdString(token.begin() + ObjectIdPathPrefix.length(), token.end());
//            TObjectId objectId;
//            if (!TObjectId::FromString(objectIdString, &objectId)) {
//                THROW_ERROR_EXCEPTION("Error parsing object id %v",
//                    objectIdString);
//            }
//            return objectId;
//        }
//
//        default:
//            tokenizer.ThrowUnexpected();
//            YT_ABORT();
//    }
//}
//
//std::optional<TResolveResult> TPortalTree::TryResolve(
//    TNodeId rootId,
//    const TYPath& path)
//{
//    auto currentPath = path;
//    TTokenizer tokenizer(currentPath);
//    int resolveDepth = 0;
//
//    auto nodeId = ParseRootId(rootId, &tokenizer);
//    auto node = FindNode(nodeId);
//    if (!node) {
//        return std::nullopt;
//    }
//
//    while (true) {
//        if (tokenizer.Advance() != ETokenType::Ampersand) {
//            if (auto* portalPayload = std::get_if<TPortalTreeNode::TPortalPayload>(&node->Payload)) {
//            }
//            if (auto* linkPayload = std::get_if<TPortalTreeNode::TLinkPayload>(&node->Payload)) {
//            }
//        }
//
//        if (const auto* )
//        switch (tokenizer.GetType()) {
//            case ETokenType::EndOfStream:
//                // xxx
//                break;
//
//            case ETokenType::Ampersand:
//                if (!std::holds_alternative<TPortalTreeNode::TMapPayload>(node->Payload)) {
//                    return std::nullopt;
//                }
//                break;
//
//            case ETokenType::Slash: {
//                tokenizer.Advance();
//                if (tokenizer.GetType() == ETokenType::Asterisk) {
//                    return std::nullopt;
//                }
//                tokenizer.Expect(ETokenType::Literal);
//                const auto& key = tokenizer.GetLiteralValue();
//                break;
//            }
//
//            default;
//                tokenizer.ThrowUnexpected();
//                YT_ABORT();
//        }
//    }
//}
//
//void TPortalTree::InvalidateNode(TNodeId nodeId)
//{
//    if (auto node = FindNode(nodeId)) {
//        node->Invalid.store(true);
//    }
//}
//
//TPortalTreeNodePtr TPortalTree::FindNode(NYT::NCypressClient::TNodeId nodeId)
//{
//    TReaderGuard guard(IdToNodeLock_);
//    auto it = IdToNode_.find(id);
//    return it == IdToNode_.end() ? nullptr : it->second;
//}
//
//////////////////////////////////////////////////////////////////////////////////
//
//} // namespace NYT::NCypressServer
