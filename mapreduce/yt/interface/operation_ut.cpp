#include <mapreduce/yt/interface/job_statistics.h>
#include <mapreduce/yt/interface/operation.h>

#include <mapreduce/yt/node/node_io.h>

#include <library/unittest/registar.h>

using namespace NYT;

class TDummyInferenceContext
    : public ISchemaInferenceContext
{
public:
    TDummyInferenceContext(int inputCount, int outputCount)
        : InputCount_(inputCount)
        , OutputCount_(outputCount)
        , InputSchemas_(inputCount)
    { }

    int GetInputTableCount() const override
    {
        return InputCount_;
    }

    int GetOutputTableCount() const override
    {
        return OutputCount_;
    }

    const TVector<TTableSchema>& GetInputTableSchemas() const override
    {
        return InputSchemas_;
    }

    const TTableSchema& GetInputTableSchema(int index) const override
    {
        return InputSchemas_[index];
    }

    TMaybe<TYPath> GetInputTablePath(int) const override
    {
        return Nothing();
    }

    TMaybe<TYPath> GetOutputTablePath(int) const override
    {
        return Nothing();
    }

private:
    int InputCount_;
    int OutputCount_;
    TVector<TTableSchema> InputSchemas_;
};

Y_UNIT_TEST_SUITE(SchemaInference)
{
    Y_UNIT_TEST(Builder)
    {
        auto someSchema = TTableSchema()
            .AddColumn(TColumnSchema().Name("some_column").Type(EValueType::VT_UINT64));
        auto otherSchema = TTableSchema()
            .AddColumn(TColumnSchema().Name("other_column").Type(EValueType::VT_BOOLEAN));
        auto thirdSchema = TTableSchema()
            .AddColumn(TColumnSchema().Name("third_column").Type(EValueType::VT_STRING));

        TDummyInferenceContext context(3,7);
        TSchemaInferenceResultBuilder builder(context);

        builder.OutputSchema(1, someSchema);
        builder.OutputSchemas(TVector<int>{2, 5}, otherSchema);
        builder.OutputSchemas(3, 5, thirdSchema);

        UNIT_ASSERT_EXCEPTION(builder.OutputSchema(1, otherSchema), TApiUsageError);
        UNIT_ASSERT_EXCEPTION(builder.OutputSchemas(3, 5, otherSchema), TApiUsageError);
        UNIT_ASSERT_EXCEPTION(builder.OutputSchemas(TVector<int>{3,6,7}, otherSchema), TApiUsageError);

        builder.RemainingOutputSchemas(thirdSchema);

        auto result = builder.Build();

        for (const auto& resultSchema : result) {
            UNIT_ASSERT(resultSchema.Defined());
        }

        UNIT_ASSERT_VALUES_EQUAL(result[0]->Columns_[0].Name_, thirdSchema.Columns_[0].Name_);
        UNIT_ASSERT_VALUES_EQUAL(result[1]->Columns_[0].Name_, someSchema.Columns_[0].Name_);
        UNIT_ASSERT_VALUES_EQUAL(result[2]->Columns_[0].Name_, otherSchema.Columns_[0].Name_);
        UNIT_ASSERT_VALUES_EQUAL(result[3]->Columns_[0].Name_, thirdSchema.Columns_[0].Name_);
        UNIT_ASSERT_VALUES_EQUAL(result[4]->Columns_[0].Name_, thirdSchema.Columns_[0].Name_);
        UNIT_ASSERT_VALUES_EQUAL(result[5]->Columns_[0].Name_, otherSchema.Columns_[0].Name_);
        UNIT_ASSERT_VALUES_EQUAL(result[6]->Columns_[0].Name_, thirdSchema.Columns_[0].Name_);
    }
} // Y_UNIT_TEST_SUITE(SchemaInference)
