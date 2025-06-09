#include "cluster.h"

#define USE_CURRENT_UDF_ABI_VERSION
#include <yql/essentials/core/yql_expr_optimize.h>

namespace NSQLComplete {

    TMaybe<TString> ToCluster(const NYql::TExprNode& node) {
        if (!node.IsCallable("DataSource") && !node.IsCallable("DataSink")) {
            return Nothing();
        }

        TStringBuf provider;
        TStringBuf cluster;
        if (node.ChildrenSize() == 2 &&
            node.Child(1)->IsAtom()) {
            provider = "";
            cluster = node.Child(1)->Content();
        } else if (node.ChildrenSize() == 3 &&
                   node.Child(1)->IsAtom() &&
                   node.Child(2)->IsAtom()) {
            provider = node.Child(1)->Content();
            cluster = node.Child(2)->Content();
        } else {
            return Nothing();
        }

        TString qualified;
        if (!provider.Empty()) {
            qualified += provider;
            qualified += ":";
        }
        qualified += cluster;
        return qualified;
    }

    THashSet<TString> CollectClusters(const NYql::TExprNode& root) {
        THashSet<TString> clusters;
        NYql::VisitExpr(root, [&](const NYql::TExprNode& node) -> bool {
            if (TMaybe<TString> cluster = ToCluster(node)) {
                clusters.emplace(std::move(*cluster));
                return true;
            }
            return true; // FIXME: try to return false
        });
        return clusters;
    }

} // namespace NSQLComplete
