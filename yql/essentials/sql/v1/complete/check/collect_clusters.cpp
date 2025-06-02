#include "collect_clusters.h"

#define USE_CURRENT_UDF_ABI_VERSION

#include <yql/essentials/core/yql_expr_optimize.h>

namespace NSQLComplete {

    THashSet<TString> CollectClusters(const NYql::TExprNode& root) {
        THashSet<TString> clusters;
        NYql::VisitExpr(root, [&](const NYql::TExprNode& node) -> bool {
            if (!node.IsCallable("DataSource") && !node.IsCallable("DataSink")) {
                return true;
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
                return false;
            }

            TString qualified;
            if (!provider.Empty()) {
                qualified += provider;
                qualified += ":";
            }
            qualified += cluster;

            clusters.emplace(std::move(qualified));
            return true;
        });
        return clusters;
    }

} // namespace NSQLComplete
