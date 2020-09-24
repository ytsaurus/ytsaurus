#include <mapreduce/yt/interface/operation.h>

#include <mapreduce/yt/tests/native/proto_lib/all_types.pb.h>
#include <mapreduce/yt/tests/native/proto_lib/all_types_proto3.pb.h>
#include <mapreduce/yt/tests/native/proto_lib/row.pb.h>

#include <mapreduce/yt/tests/native/ydl_lib/row.ydl.h>
#include <mapreduce/yt/tests/native/ydl_lib/all_types.ydl.h>

namespace NYT::NTesting {

namespace NYdlRows = mapreduce::yt::tests::native::ydl_lib::row;
namespace NYdlAllTypes = mapreduce::yt::tests::native::ydl_lib::all_types;

////////////////////////////////////////////////////////////////////////////////

class TAlwaysFailingMapper : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter*)
    {
        for (; reader->IsValid(); reader->Next()) {
        }
        Cerr << "This mapper always fails" << Endl;
        ::exit(1);
    }
};

class TIdMapper : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer)
    {
        for (; reader->IsValid(); reader->Next()) {
            writer->AddRow(reader->GetRow());
        }
    }
};

class TIdReducer : public IReducer<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer)
    {
        for (; reader->IsValid(); reader->Next()) {
            writer->AddRow(reader->GetRow());
        }
    }
};

class TSleepingMapper : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    TSleepingMapper() = default;

    TSleepingMapper(TDuration sleepDuration)
        : SleepDuration_(sleepDuration)
    { }

    virtual void Do(TReader*, TWriter* ) override
    {
        Sleep(SleepDuration_);
    }

    Y_SAVELOAD_JOB(SleepDuration_);

private:
    TDuration SleepDuration_;
};

class THugeStderrMapper : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    THugeStderrMapper() = default;
    virtual void Do(TReader*, TWriter*) override {
        TString err(1024 * 1024 * 10, 'a');
        Cerr.Write(err);
        Cerr.Flush();
        exit(1);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TUrlRowIdMapper : public IMapper<TTableReader<TUrlRow>, TTableWriter<TUrlRow>>
{
public:
    void Do(TReader* reader, TWriter* writer)
    {
        for (; reader->IsValid(); reader->Next()) {
            writer->AddRow(reader->GetRow());
        }
    }
};

class TYdlUrlRowIdMapper : public IMapper<TTableReader<NYdlRows::TUrlRow>, TTableWriter<NYdlRows::TUrlRow>>
{
public:
    void Do(TReader* reader, TWriter* writer)
    {
        for (; reader->IsValid(); reader->Next()) {
            writer->AddRow(reader->GetRow());
        }
    }
};

class TUrlRowIdReducer : public IReducer<TTableReader<TUrlRow>, TTableWriter<TUrlRow>>
{
public:
    void Do(TReader* reader, TWriter* writer)
    {
        for (; reader->IsValid(); reader->Next()) {
            writer->AddRow(reader->GetRow());
        }
    }
};

class TYdlUrlRowIdReducer : public IReducer<TTableReader<NYdlRows::TUrlRow>, TTableWriter<NYdlRows::TUrlRow>>
{
public:
    void Do(TReader* reader, TWriter* writer)
    {
        for (; reader->IsValid(); reader->Next()) {
            writer->AddRow(reader->GetRow());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMapperThatWritesStderr : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    TMapperThatWritesStderr() = default;

    TMapperThatWritesStderr(TStringBuf str)
        : Stderr_(str)
    { }

    void Do(TReader* reader, TWriter*) override {
        for (; reader->IsValid(); reader->Next()) {
        }
        Cerr << Stderr_;
    }

    Y_SAVELOAD_JOB(Stderr_);

private:
    TString Stderr_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTesting
