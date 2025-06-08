#include "collect.h"

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

    TMaybe<TString> ToTablePath(const NYql::TExprNode& node) {
        if (node.IsCallable("MrTableConcat")) {
            Y_ENSURE(node.ChildrenSize() < 2);
            Cerr << "Opa: MrTableConcat" << Endl;
            return ToTablePath(*node.Child(0));
        }

        if (!node.IsCallable("Key") || node.ChildrenSize() < 1) {
            Cerr << "Oops: Key problem at " << node.Content() << Endl;
            return Nothing();
        }

        const NYql::TExprNode* table = node.Child(0);
        if (!table->IsList() || table->ChildrenSize() < 2) {
            Cerr << "Oops: List problem" << Endl;
            return Nothing();
        }

        TStringBuf kind = table->Child(0)->Content();
        if (kind != "table" && kind != "tablescheme") {
            Cerr << "Oops: table/tablescheme problem" << Endl;
            return Nothing();
        }

        const NYql::TExprNode* string = table->Child(1);
        if (!string->IsCallable("String") || string->ChildrenSize() < 1) {
            Cerr << "Oops: String problem at " << string->Content() << Endl;
            return Nothing();
        }

        return TString(string->Child(0)->Content());
    }

} // namespace NSQLComplete
