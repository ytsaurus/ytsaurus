#include <mapreduce/yt/io/proxy_input.h>
#include <mapreduce/yt/io/yamr_table_reader.h>

#include <library/unittest/registar.h>

using namespace NYT;

template <>
void Out<std::tuple<TString, TString, TString>>(IOutputStream& out, const std::tuple<TString, TString, TString>& value) {
    out << "{" << std::get<0>(value) << ", " << std::get<1>(value) << ", " << std::get<2>(value) << "}";
}


////////////////////////////////////////////////////////////////////

class TRowCollection
{
public:
    void AddRow(TStringBuf key, TStringBuf subkey, TStringBuf value)
    {
        TStringStream row;
        auto appendLenval = [&] (TStringBuf value) {
            ui32 size = value.Size();
            row.Write(&size, sizeof(size));
            row.Write(value);
        };
        appendLenval(key);
        appendLenval(subkey);
        appendLenval(value);
        RowList_.push_back(row.Str());
    }

    TString GetStreamDataStartFromRow(ui64 rowIndex) const
    {
        Y_VERIFY(rowIndex < RowList_.size());
        TStringStream ss;
        ss.Write("\xFC\xFF\xFF\xFF");
        ss.Write(&rowIndex, sizeof(rowIndex));
        for (size_t i = rowIndex; i != RowList_.size(); ++i) {
            ss.Write(RowList_[i]);
        }
        return ss.Str();
    }

    size_t ComputeTotalStreamSize() const {
        return GetStreamDataStartFromRow(0).size();
    }

private:
    TVector<TString> RowList_;
};

class TTestProxyInput
    : public TProxyInput
{
public:
    TTestProxyInput(const TRowCollection& rowCollection)
        : RowCollection_(rowCollection)
        , DataToRead_(RowCollection_.GetStreamDataStartFromRow(0))
        , Input_(MakeHolder<TStringStream>(DataToRead_))
    { }

    TTestProxyInput(const TRowCollection& rowCollection, size_t breakPoint)
        : RowCollection_(rowCollection)
        , DataToRead_(RowCollection_.GetStreamDataStartFromRow(0).substr(0, breakPoint))
        , Input_(MakeHolder<TStringStream>(DataToRead_))
        , Broken_(true)
    { }

    size_t DoRead(void* buf, size_t size) override
    {
        Y_VERIFY(Input_);
        size_t res = Input_->Read(buf, size);
        if (!res && Broken_) {
            ythrow yexception() << "Stream is broken";
        }
        return res;
    }

    bool Retry(
        const TMaybe<ui32>& /*rangeIndex*/,
        const TMaybe<ui64>& rowIndex) override
    {
        if (--Retries < 0) {
            return false;
        }
        ui64 actualRowIndex = rowIndex ? *rowIndex : 0;
        DataToRead_ = RowCollection_.GetStreamDataStartFromRow(actualRowIndex);
        Input_ = MakeHolder<TStringInput>(DataToRead_);
        Broken_ = false;
        return true;
    }

private:
    TRowCollection RowCollection_;
    TString DataToRead_;
    THolder<IInputStream> Input_;
    bool Broken_ = false;
    i32 Retries = 1;
};

SIMPLE_UNIT_TEST_SUITE(TestYamrTableReader)
{
    SIMPLE_UNIT_TEST(TestReadRetry)
    {
        const TVector<std::tuple<TString, TString, TString>> expectedResult = {
            {"foo1", "bar1", "baz1"},
            {"foo2", "bar2", "baz2"},
            {"foo3", "bar3", "baz3"},
        };

        TRowCollection rowCollection;
        for (const auto& row : expectedResult) {
            rowCollection.AddRow(std::get<0>(row), std::get<1>(row), std::get<2>(row));
        }

        ssize_t streamSize = rowCollection.ComputeTotalStreamSize();

        for (ssize_t breakPoint = -1; breakPoint < streamSize; ++breakPoint) {
            ::TIntrusivePtr<TProxyInput> proxyInput;
            if (breakPoint == -1) {
                proxyInput = ::MakeIntrusive<TTestProxyInput>(rowCollection);
            } else {
                proxyInput = ::MakeIntrusive<TTestProxyInput>(rowCollection, static_cast<size_t>(breakPoint));
            }

            TYaMRTableReader tableReader(proxyInput);
            TVector<std::tuple<TString, TString, TString>> actualResult;
            for (; tableReader.IsValid(); tableReader.Next()) {
                auto row = tableReader.GetRow();
                actualResult.emplace_back(row.Key, row.SubKey, row.Value);
            }
            UNIT_ASSERT_VALUES_EQUAL(actualResult, expectedResult);
        }
    }

    SIMPLE_UNIT_TEST(TestSkipRetry)
    {
        const TVector<std::tuple<TString, TString, TString>> expectedResult = {
            {"foo1", "bar1", "baz1"},
            {"foo2", "bar2", "baz2"},
            {"foo3", "bar3", "baz3"},
        };

        TRowCollection rowCollection;
        for (const auto& row : expectedResult) {
            rowCollection.AddRow(std::get<0>(row), std::get<1>(row), std::get<2>(row));
        }

        ssize_t streamSize = rowCollection.ComputeTotalStreamSize();

        for (ssize_t breakPoint = -1; breakPoint < streamSize; ++breakPoint) {
            try {
                ::TIntrusivePtr<TProxyInput> proxyInput;
                if (breakPoint == -1) {
                    proxyInput = ::MakeIntrusive<TTestProxyInput>(rowCollection);
                } else {
                    proxyInput = ::MakeIntrusive<TTestProxyInput>(rowCollection, static_cast<size_t>(breakPoint));
                }

                TYaMRTableReader tableReader(proxyInput);
                ui32 rowCount = 0;
                for (; tableReader.IsValid(); tableReader.Next()) {
                    ++rowCount;
                }
                UNIT_ASSERT_VALUES_EQUAL(rowCount, 3);
            } catch (const yexception& ex) {
                Cerr << breakPoint << Endl;
                Cerr << ex.what() << Endl;
                throw;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////
