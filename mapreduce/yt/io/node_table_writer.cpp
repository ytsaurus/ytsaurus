#include "node_table_writer.h"

#include "proxy_output.h"

#include <mapreduce/yt/yson/writer.h>
#include <mapreduce/yt/common/node_visitor.h>
#include <mapreduce/yt/common/log.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TNodeTableWriter::TNodeTableWriter(THolder<TProxyOutput> output)
    : Output_(std::move(output))
{
    for (size_t i = 0; i < Output_->GetStreamCount(); ++i) {
        Writers_.push_back(
            new TYsonWriter(Output_->GetStream(i), YF_BINARY, YT_LIST_FRAGMENT));
    }
}

TNodeTableWriter::~TNodeTableWriter()
{ }

void TNodeTableWriter::AddRow(const TNode& row, size_t tableIndex)
{
    if (tableIndex >= Writers_.size()) {
        ythrow TIOException() <<
            Sprintf("Table index %" PRISZT " is out of range [0, %" PRISZT ")",
                tableIndex, Writers_.size());
    }

    if (row.HasAttributes()) {
        ythrow TIOException() << "Row cannot have attributes";
    }

    static const TNode emptyMap = TNode::CreateMap();
    const TNode* outRow = &emptyMap;
    if (row.GetType() != TNode::UNDEFINED) {
        if (!row.IsMap()) {
            ythrow TIOException() << "Row should be a map node";
        } else {
            outRow = &row;
        }
    }

    auto* writer = Writers_[tableIndex].Get();
    writer->OnListItem();
    TNodeVisitor visitor(writer);
    visitor.Visit(*outRow);
    Output_->OnRowFinished(tableIndex);
}

void TNodeTableWriter::Finish()
{
    for (size_t i = 0; i < Output_->GetStreamCount(); ++i) {
        Output_->GetStream(i)->Finish();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
