#include "collect_clusters.h"

#include "collect.h"

#define USE_CURRENT_UDF_ABI_VERSION
#include <yql/essentials/core/yql_expr_optimize.h>

namespace NSQLComplete {

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
