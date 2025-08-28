#include "helpers.h"

namespace NYT::NScheduler {

using namespace NYPath;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TYsonMapFragmentBatcher::TYsonMapFragmentBatcher(
    std::vector<NYson::TYsonString>* batchOutput,
    int maxBatchSize,
    EYsonFormat format)
    : BatchOutput_(batchOutput)
    , MaxBatchSize_(maxBatchSize)
    , BatchWriter_(CreateYsonWriter(&BatchStream_, format, EYsonType::MapFragment, /*enableRaw*/ false))
{ }

void TYsonMapFragmentBatcher::Flush()
{
    BatchWriter_->Flush();

    if (BatchStream_.empty()) {
        return;
    }

    BatchOutput_->push_back(TYsonString(BatchStream_.Str(), EYsonType::MapFragment));
    BatchSize_ = 0;
    BatchStream_.clear();
}

void TYsonMapFragmentBatcher::OnMyKeyedItem(TStringBuf key)
{
    BatchWriter_->OnKeyedItem(key);
    Forward(
        BatchWriter_.get(),
        /*onFinished*/ [&] {
            ++BatchSize_;
            if (BatchSize_ == MaxBatchSize_) {
                Flush();
            }
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
