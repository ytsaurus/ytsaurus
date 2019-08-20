#include "operation_description.h"

#include <yt/core/misc/shutdown.h>

#include <yt/core/logging/config.h>
#include <yt/core/logging/log.h>
#include <yt/core/logging/log_manager.h>

#include <yt/core/misc/phoenix.h>

#include <yt/core/profiling/timing.h>

#include <yt/core/yson/lexer.h>
#include <yt/core/yson/parser.h>
#include <yt/core/yson/writer.h>
#include <yt/core/yson/null_consumer.h>
#include <yt/core/yson/stream.h>

#include <util/system/fs.h>

#include <iostream>

using namespace NYT;
using namespace NSchedulerSimulator;
using namespace NYTree;
using namespace NYson;
using namespace NPhoenix;

NLogging::TLogger Logger("Converter");

template <class T>
class TYsonListExtractor
    : public TForwardingYsonConsumer
{
public:
    DEFINE_BYVAL_RO_PROPERTY(int, ExtractedCount);

public:
    TYsonListExtractor(const std::function<void(const T&)>& onEntryExtracted)
        : ExtractedCount_(0)
        , OnEntryExtracted_(onEntryExtracted)
    { }

    virtual void OnMyListItem() override
    {
        if (Builder_) {
            ExtractEntry();
        }
        Builder_ = CreateBuilderFromFactory(GetEphemeralNodeFactory());
        Builder_->BeginTree();
        Forward(Builder_.get());
    }

    void Finish()
    {
        if (Builder_) {
            ExtractEntry();
            Builder_.reset();
        }
    }

private:
    void ExtractEntry()
    {
        auto node = Builder_->EndTree();
        OnEntryExtracted_(ConvertTo<T>(node));
        ++ExtractedCount_;
        if (ExtractedCount_ % 1000 == 0) {
            YT_LOG_INFO("Records extracted: %v", ExtractedCount_);
        }
    }

    std::function<void(const T&)> OnEntryExtracted_;
    std::unique_ptr<ITreeBuilder> Builder_;
};

int main(int argc, char** argv)
{
    if (argc != 2) {
        std::cout << "Usage: " << argv[0] << " DESTINATION" << std::endl;
        return 0;
    }
    TString destination(argv[1]);
    TString destinationTemp(destination + ".tmp");

    {
        auto input = TYsonInput(&Cin, NYson::EYsonType::ListFragment);
        TSaveContext context;
        TUnbufferedFileOutput outputTemp(destinationTemp);
        context.SetOutput(&outputTemp);
        TYsonListExtractor<TOperationDescription> extractor(
            [&] (const TOperationDescription& entry) { Save(context, entry); });

        Serialize(input, &extractor);
        extractor.Finish();

        int extractedCount = extractor.GetExtractedCount();
        TUnbufferedFileOutput output(destination);
        output.Write(&extractedCount, sizeof extractedCount);
    }
    NFs::Cat(destination.data(), destinationTemp.data());
    NFs::Remove(destinationTemp.data());

    NYT::Shutdown();
}
