#include "stdafx.h"
#include "graphviz.h"

#include "private.h"
#include "helpers.h"

#include "plan_node.h"
#include "plan_visitor.h"
#include "plan_fragment.h"
#include "plan_context.h"

#include <core/misc/assert.h>
#include <core/misc/process.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/unversioned_row.h>

#include <util/system/defaults.h>
#include <util/system/file.h>
#include <util/stream/file.h>

#include <sys/types.h>

// Required in printing visitor.
#include <core/misc/guid.h>
#include <core/misc/protobuf_helpers.h>

// Required for std::unordered_set.
#ifdef _LIBCPP_BEGIN_NAMESPACE_STD
_LIBCPP_BEGIN_NAMESPACE_STD
#else
namespace std {
#endif
template <>
struct hash<std::pair<const void*, const void*>>
{
    std::size_t operator () (const std::pair<const void*, const void*>& pair) const
    {
        return (size_t)pair.first + (size_t)pair.second * 17;
    }
};
#ifdef _LIBCPP_END_NAMESPACE_STD
_LIBCPP_END_NAMESPACE_STD
#else
} // namespace std
#endif

namespace NYT {
namespace NDot {

////////////////////////////////////////////////////////////////////////////////

Stroka EscapeString(const Stroka& s)
{
    Stroka r(s.begin(), s.end());

    for (size_t i = 0; i < r.length(); ++i) {
        switch (r[i]) {
            case '\n':
                r.insert(r.begin() + i, '\\');
                ++i;
                r.replace(i, 1, 1, 'n'); // r[i] = 'n';
                break;
            case '\t':
                r.insert(r.begin() + i, ' ');
                ++i;
                r.replace(i, 1, 1, ' '); // r[i] = ' ';
            case '\\':
                if (i + 1 != r.length()) {
                    switch (r[i + 1]) {
                        case 'l': continue;
                        case '|':
                        case '{':
                        case '}':
                            r.erase(r.begin() + i);
                            continue;
                        default:
                            break;
                    }
                }
            case '{':
            case '}':
            case '<':
            case '>':
            case '|':
            case '"':
                r.insert(r.begin() + i, '\\');
                ++i;
                break;
        }
    }

    return r;
}

Stroka EscapeHtml(const Stroka& s)
{
    Stroka r(s.begin(), s.end());
    size_t i;

    auto replaceWith = [&] (const char* replacement) {
        r.erase(r.begin() + i);
        r.insert(i, replacement);
        i += strlen(replacement);
    };

    for (i = 0; i < r.length(); ++i) {
        switch (r[i]) {
            case '\n':
                replaceWith("<BR/>");
                break;
            case '<':
                replaceWith("&lt;");
                break;
            case '>':
                replaceWith("&gt;");
                break;
            case '&':
                replaceWith("&amp;");
                break;
        }
    }

    return r;
}

#ifndef _win_

void ViewGraph(const Stroka& file)
{
    TProcess p("/usr/bin/xdot");
    p.AddArgument("xdot");
    p.AddArgument("-f");
    p.AddArgument("dot");
    p.AddArgument(~file);

    auto error = p.Spawn();
    YCHECK(error.IsOK());

    error = p.Wait();
    YCHECK(error.IsOK());
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NDot
} // namespace NYT

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using ::ToString;

template <class T, class = void>
struct TGraphVizTraits;

template <class T>
struct TGraphVizTraits<
    T,
    typename std::enable_if<std::is_base_of<TOperator, T>::value>::type>
{
    static const int UniqueId = 1;
    static inline const char* GetPrefix() { return "Op"; }
};

template <class T>
struct TGraphVizTraits<
    T,
    typename std::enable_if<std::is_base_of<TExpression, T>::value>::type>
{
    static const int UniqueId = 2;
    static inline const char* GetPrefix() { return "Expr"; }
};

class TGraphVizVisitor
    : public IPlanVisitor
{
public:
    TGraphVizVisitor(TOutputStream& output)
        : Output_(output)
    { }

    template <class TNode>
    Stroka GetName(TNode* node, const Stroka& port = "")
    {
        auto prefix = TGraphVizTraits<TNode>::GetPrefix();
        if (port.empty()) {
            return Format("%v%v", prefix, node);
        } else {
            return Format("%v%v:%v", prefix, node, port);
        }
    }

    void WriteHeader(const Stroka& title)
    {
        Output_ << "digraph unnamed {\n";
        Output_ << "\tlabel=\"" << NDot::EscapeString(title) << "\";\n";
        Output_ << "\tfontname=Courier;\n";
        Output_ << "\tnode [shape=none,colorscheme=pastel19,fontname=Courier];\n";
        Output_ << "\tedge [shape=solid,fontname=Courier];\n";
        Output_ << "\n";
    }

    void WriteFooter()
    {
        Output_ << "}\n";
    }

    template <class TNode>
    void WriteNode(TNode* node, const Stroka& label, const Stroka& attributes = "")
    {
        if (VisitedNodes_.find(node) == VisitedNodes_.end()) {
            VisitedNodes_.insert(node);
        } else {
            return;
        }

        Output_ << "\t" << GetName(node) << " [";
        if (!attributes.empty()) {
            Output_ << attributes << ",";
        }
        Output_ << "label=<" << label << ">];\n";
    }

    template <class TFrom, class TTo>
    void WriteEdge(
        TFrom* from,
        TTo* to,
        const Stroka& fromPort = "",
        const Stroka& toPort = "")
    {
        auto pair = std::make_pair(from, to);
        if (VisitedEdges_.find(pair) == VisitedEdges_.end()) {
            VisitedEdges_.insert(pair);
        } else {
            return;
        }

        static const bool constrained =
            (TGraphVizTraits<TFrom>::UniqueId == TGraphVizTraits<TTo>::UniqueId);

        Output_ << "\t" << GetName(from, fromPort) << " -> " << GetName(to, toPort);
        if (!constrained) {
            Output_ << " [style=dotted,constraint=false]";
        }
        Output_ << ";\n";
    }

    struct TLabel
    {
        Stroka Value_;

        TLabel(const TOperator* op)
        {
            AddHeader(op);
            WithRow(
                "KeyColumns: " + NDot::EscapeHtml(JoinToString(op->GetKeyColumns())));
        }

        TLabel(const TExpression* expr, const TTableSchema& sourceSchema)
        {
            AddHeader(expr);
            WithRow(
                "Type: " + ToString(expr->GetType(sourceSchema)) + "<BR/>" +
                "Name: " + NDot::EscapeHtml(expr->GetName()));
        }

        template <class TNode>
        void AddHeader(const TNode* node)
        {
            Value_ += "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">";
            Value_ += Format(
                "<TR><TD BGCOLOR=\"//%d\">%v</TD></TR>",
                TGraphVizTraits<TNode>::UniqueId,
                node->GetKind());
        }

        void AddFooter()
        {
            Value_ += "</TABLE>";
        }

        TLabel& WithRow(const Stroka& row)
        {
            Value_ += "<TR><TD ALIGN=\"LEFT\" BALIGN=\"LEFT\">";
            Value_ += row;
            Value_ += "</TD></TR>";
            return *this;
        }

        TLabel& WithPortAndRow(const Stroka& port, const Stroka& row)
        {
            Value_ += "<TR><TD ALIGN=\"LEFT\" BALIGN=\"LEFT\" ";
            Value_ += "PORT=\"" + NDot::EscapeHtml(port) + "\">";
            Value_ += row;
            Value_ += "</TD></TR>";
            return *this;
        }

        Stroka Build()
        {
            AddFooter();
            return Value_;
        }
    };

    virtual bool Visit(const TScanOperator* op) override
    {
        CurrentSourceSchema_ = op->GetTableSchema();
        using NObjectClient::TObjectId;
        using NObjectClient::TypeFromId;

        Stroka dataSplitsInfo;
        dataSplitsInfo += "Splits: [";
        for (const auto& dataSplit : op->DataSplits()) {
            auto objectId = GetObjectIdFromDataSplit(dataSplit);
            if (!dataSplitsInfo.empty()) {
                dataSplitsInfo += ", ";
            }
            dataSplitsInfo += Stroka() + "{" +
                "<BR/>Id: " + ToString(objectId) +
                "<BR/>Type: " + ToString(TypeFromId(objectId)) +
                "<BR/>Sorted: " + (IsSorted(dataSplit) ? "true" : "false") +
                "<BR/>}";
        }
        dataSplitsInfo += "]";

        WriteNode(
            op,
            TLabel(op)
                .WithRow(dataSplitsInfo)
                .Build());

        return true;
    }

    virtual bool Visit(const TFilterOperator* op) override
    {
        CurrentSourceSchema_ = op->GetSource()->GetTableSchema();
        WriteNode(
            op,
            TLabel(op)
                .WithPortAndRow("p",
                    "[P]: " + NDot::EscapeHtml(op->GetPredicate()->GetSource()))
                .Build());
        WriteEdge(op, op->GetSource());
        WriteEdge(op, op->GetPredicate(), "p");
        Traverse(this, op->GetPredicate());
        return true;
    }

    virtual bool Visit(const TGroupOperator* op) override
    {
        CurrentSourceSchema_ = op->GetSource()->GetTableSchema();

        int i = 0;

        TLabel label(op);
        for (int j = 0; j < op->GetGroupItemCount(); ++i, ++j) {
            const auto& item = op->GetGroupItem(j);
            label.WithPortAndRow(
                ToString(i),
                Format("G_%v[%v]: %s",
                    j,
                    item.Name,
                    NDot::EscapeHtml(item.Expression->GetSource())));
        }
        for (int j = 0; j < op->GetAggregateItemCount(); ++i, ++j) {
            const auto& item = op->GetAggregateItem(j);
            label.WithPortAndRow(
                ToString(i),
                Format("A_%v[%v]: %v(%v)",
                    j,
                    item.Name,
                    item.AggregateFunction,
                    NDot::EscapeHtml(item.Expression->GetSource())));
        }
        WriteNode(op, label.Build());
        WriteEdge(op, op->GetSource());

        i = 0;

        for (int j = 0; j < op->GetGroupItemCount(); ++i, ++j) {
            auto* expr = op->GetGroupItem(j).Expression;
            WriteEdge(op, expr, ToString(i));
            Traverse(this, expr);
        }
        for (int j = 0; j < op->GetAggregateItemCount(); ++i, ++j) {
            auto* expr = op->GetAggregateItem(j).Expression;
            WriteEdge(op, expr, ToString(i));
            Traverse(this, expr);
        }

        return true;
    }

    virtual bool Visit(const TProjectOperator* op) override
    {
        CurrentSourceSchema_ = op->GetSource()->GetTableSchema();
        TLabel label(op);
        for (int i = 0; i < op->GetProjectionCount(); ++i) {
            label.WithPortAndRow(
                ToString(i),
                Format("[%v]: %v",
                    i,
                    NDot::EscapeHtml(op->GetProjection(i).Expression->GetSource())));
        }
        WriteNode(op, label.Build());
        WriteEdge(op, op->GetSource());
        for (int i = 0; i < op->GetProjectionCount(); ++i) {
            WriteEdge(op, op->GetProjection(i).Expression, ToString(i));
            Traverse(this, op->GetProjection(i).Expression);
        }
        return true;
    }

    virtual bool Visit(const TLiteralExpression* expr) override
    {
        WriteNode(
            expr,
            TLabel(expr, CurrentSourceSchema_).WithRow(ToString(expr->GetValue())).Build());
        return true;
    }

    virtual bool Visit(const TReferenceExpression* expr) override
    {
        WriteNode(
            expr,
            TLabel(expr, CurrentSourceSchema_)
                .WithRow("ColumnName: " + expr->GetColumnName() + "<BR/>")
                .Build());
        return true;
    }

    virtual bool Visit(const TFunctionExpression* expr) override
    {
        TLabel label(expr, CurrentSourceSchema_);
        label.WithRow("FunctionName: " + expr->GetFunctionName());
        for (int i = 0; i < expr->GetArgumentCount(); ++i) {
            label.WithPortAndRow(
                ToString(i),
                Format("[%v]: %v",
                    i,
                    NDot::EscapeHtml(expr->GetArgument(i)->GetSource())));
        }
        WriteNode(expr, label.Build());
        for (int i = 0; i < expr->GetArgumentCount(); ++i) {
            WriteEdge(expr, expr->GetArgument(i), ToString(i));
            Traverse(this, expr->GetArgument(i));
        }
        return true;
    }

    virtual bool Visit(const TBinaryOpExpression* expr) override
    {
        WriteNode(
            expr,
            TLabel(expr, CurrentSourceSchema_)
                .WithRow("Opcode: " + ToString(expr->GetOpcode()))
                .Build());
        WriteEdge(expr, expr->GetLhs());
        WriteEdge(expr, expr->GetRhs());
        return true;
    }

private:
    TOutputStream& Output_;
    std::unordered_set<const void*> VisitedNodes_;
    std::unordered_set<std::pair<const void*, const void*>> VisitedEdges_;
    TTableSchema CurrentSourceSchema_;

};

////////////////////////////////////////////////////////////////////////////////

void DumpPlanFragment(
    const TPlanFragment& fragment,
    TOutputStream& output,
    const Stroka& title)
{
    auto source = fragment.GetContext()->GetSource();
    auto headOperator = fragment.GetHead();

    TGraphVizVisitor visitor(output);

    auto actualTitle = title.empty() ? source : title;

    visitor.WriteHeader(title);
    Traverse(&visitor, headOperator);
    visitor.WriteFooter();
}

void DumpPlanFragmentToFile(
    const TPlanFragment& fragment,
    const Stroka& file,
    const Stroka& title)
{
    TFileOutput output(file);
    DumpPlanFragment(fragment, output, title);
}

void ViewPlanFragment(const TPlanFragment& fragment, const Stroka& title)
{
#ifndef _win_
    char file[] = "/tmp/graph.XXXXXX";
    int fd = mkstemp(file);

    YCHECK(fd > 0);

    try {
        TFile handle(fd);
        TFileOutput output(handle);

        DumpPlanFragment(fragment, output, title);
        NDot::ViewGraph(file);

        ::unlink(file);
    } catch (...) {
        ::unlink(file);
        throw;
    }
#else
    YUNIMPLEMENTED();
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
