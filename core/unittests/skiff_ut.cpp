#include <yt/core/test_framework/framework.h>

#include <yt/core/skiff/skiff.h>
#include <yt/core/skiff/skiff_schema.h>

#include <util/stream/buffer.h>

using namespace NYT::NSkiff;

////////////////////////////////////////////////////////////////////////////////

TEST(TSkiffTest, TestInt)
{
    TBufferStream bufferStream;

    auto schema = CreateSimpleTypeSchema(EWireType::Uint64);

    TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
    tokenWriter.WriteUint64(42);
    tokenWriter.WriteUint64(100500);

    tokenWriter.Finish();

    TCheckedSkiffParser parser(schema, &bufferStream);

    ASSERT_EQ(parser.ParseUint64(), 42);
    ASSERT_EQ(parser.ParseUint64(), 100500);
}

TEST(TSkiffTest, TestBoolean)
{
    auto schema = CreateSimpleTypeSchema(EWireType::Boolean);

    TBufferStream bufferStream;
    TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
    tokenWriter.WriteBoolean(true);
    tokenWriter.WriteBoolean(false);
    tokenWriter.Finish();

    TCheckedSkiffParser parser(schema, &bufferStream);
    ASSERT_EQ(parser.ParseBoolean(), true);
    ASSERT_EQ(parser.ParseBoolean(), false);

    {
        TBufferStream bufferStream;
        bufferStream.Write('\x02');

        TCheckedSkiffParser parser(schema, &bufferStream);
        ASSERT_THROW(parser.ParseBoolean(), std::exception);
    }
};

TEST(TSkiffTest, TestVariant8)
{
    auto schema = CreateVariant8Schema({
        CreateSimpleTypeSchema(EWireType::Nothing),
        CreateSimpleTypeSchema(EWireType::Uint64),
    });

    {
        TBufferStream bufferStream;
        TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
        ASSERT_THROW(tokenWriter.WriteUint64(42), std::exception);
    }

    {
        TBufferStream bufferStream;
        TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
        tokenWriter.WriteVariant8Tag(0);
        ASSERT_THROW(tokenWriter.WriteUint64(42), std::exception);
    }
    {
        TBufferStream bufferStream;
        TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
        tokenWriter.WriteVariant8Tag(1);
        ASSERT_THROW(tokenWriter.WriteInt64(42), std::exception);
    }

    TBufferStream bufferStream;
    TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
    tokenWriter.WriteVariant8Tag(0);
    tokenWriter.WriteVariant8Tag(1);
    tokenWriter.WriteUint64(42);
    tokenWriter.Finish();

    TCheckedSkiffParser parser(schema, &bufferStream);
    ASSERT_EQ(parser.ParseVariant8Tag(), 0);
    ASSERT_EQ(parser.ParseVariant8Tag(), 1);
    ASSERT_EQ(parser.ParseUint64(), 42);

    parser.ValidateFinished();
}

TEST(TSkiffTest, TestTuple)
{

    auto schema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Int64),
        CreateSimpleTypeSchema(EWireType::String32),
    });

    {
        TBufferStream bufferStream;

        TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
        tokenWriter.WriteInt64(42);
        tokenWriter.WriteString32("foobar");
        tokenWriter.Finish();

        TCheckedSkiffParser parser(schema, &bufferStream);
        ASSERT_EQ(parser.ParseInt64(), 42);
        ASSERT_EQ(parser.ParseString32(), "foobar");
        parser.ValidateFinished();
    }
}

TEST(TSkiffTest, TestString)
{

    auto schema = CreateSimpleTypeSchema(EWireType::String32);

    {
        TBufferStream bufferStream;

        TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
        tokenWriter.WriteString32("foo");
        tokenWriter.Finish();

        TCheckedSkiffParser parser(schema, &bufferStream);

        ASSERT_EQ(parser.ParseString32(), "foo");

        parser.ValidateFinished();
    }

    {
        TBufferStream bufferStream;

        TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
        tokenWriter.WriteString32("foo");
        tokenWriter.Finish();

        TCheckedSkiffParser parser(schema, &bufferStream);
        ASSERT_THROW(parser.ParseInt64(), std::exception);
    }
}

TEST(TSkiffTest, TestRepeatedVariant16)
{

    auto schema = CreateRepeatedVariant16Schema({
        CreateSimpleTypeSchema(EWireType::Int64),
        CreateSimpleTypeSchema(EWireType::Uint64),
    });

    {
        TBufferStream bufferStream;
        TCheckedSkiffWriter tokenWriter(schema, &bufferStream);

        // row 0
        tokenWriter.WriteVariant16Tag(0);
        tokenWriter.WriteInt64(-8);

        // row 2
        tokenWriter.WriteVariant16Tag(1);
        tokenWriter.WriteUint64(42);

        // end
        tokenWriter.WriteVariant16Tag(EndOfSequenceTag<ui16>());

        tokenWriter.Finish();

        TCheckedSkiffParser parser(schema, &bufferStream);

        // row 1
        ASSERT_EQ(parser.ParseVariant16Tag(), 0);
        ASSERT_EQ(parser.ParseInt64(), -8);

        // row 2
        ASSERT_EQ(parser.ParseVariant16Tag(), 1);
        ASSERT_EQ(parser.ParseUint64(), 42);

        // end
        ASSERT_EQ(parser.ParseVariant16Tag(), EndOfSequenceTag<ui16>());

        parser.ValidateFinished();
    }

    {
        TBufferStream bufferStream;
        TCheckedSkiffWriter tokenWriter(schema, &bufferStream);

        tokenWriter.WriteVariant16Tag(0);
        ASSERT_THROW(tokenWriter.WriteUint64(5), std::exception);
    }

    {
        TBufferStream bufferStream;
        TCheckedSkiffWriter tokenWriter(schema, &bufferStream);

        tokenWriter.WriteVariant16Tag(1);
        tokenWriter.WriteUint64(5);

        ASSERT_THROW(tokenWriter.Finish(), std::exception);
    }

    {
        TBufferStream bufferStream;
        TCheckedSkiffWriter tokenWriter(schema, &bufferStream);

        // row 0
        tokenWriter.WriteVariant16Tag(0);
        tokenWriter.WriteInt64(-8);

        // row 2
        tokenWriter.WriteVariant16Tag(1);
        tokenWriter.WriteUint64(42);

        // end
        tokenWriter.WriteVariant16Tag(EndOfSequenceTag<ui16>());

        tokenWriter.Finish();

        {
            TBufferInput input(bufferStream.Buffer());
            TCheckedSkiffParser parser(schema, &input);

            ASSERT_THROW(parser.ParseInt64(), std::exception);
        }

        {
            TBufferInput input(bufferStream.Buffer());
            TCheckedSkiffParser parser(schema, &input);

            parser.ParseVariant16Tag();
            ASSERT_THROW(parser.ParseUint64(), std::exception);
        }

        {
            TBufferInput input(bufferStream.Buffer());
            TCheckedSkiffParser parser(schema, &input);

            parser.ParseVariant16Tag();
            parser.ParseInt64();

            ASSERT_THROW(parser.ValidateFinished(), std::exception);
        }
    }
}

TEST(TSkiffTest, TestStruct)
{
    TBufferStream bufferStream;

    auto schema = CreateRepeatedVariant16Schema(
        {
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateTupleSchema({
                CreateVariant8Schema({
                    CreateSimpleTypeSchema(EWireType::Nothing),
                    CreateSimpleTypeSchema(EWireType::Int64)
                }),
                CreateSimpleTypeSchema(EWireType::Uint64),
            })
        }
    );

    {
        TCheckedSkiffWriter tokenWriter(schema, &bufferStream);

        // row 0
        tokenWriter.WriteVariant16Tag(0);

        // row 1
        tokenWriter.WriteVariant16Tag(1);
        tokenWriter.WriteVariant8Tag(0);
        tokenWriter.WriteUint64(1);

        // row 2
        tokenWriter.WriteVariant16Tag(1);
        tokenWriter.WriteVariant8Tag(1);
        tokenWriter.WriteInt64(2);
        tokenWriter.WriteUint64(3);

        // end
        tokenWriter.WriteVariant16Tag(EndOfSequenceTag<ui16>());

        tokenWriter.Finish();
    }

    TCheckedSkiffParser parser(schema, &bufferStream);

    // row 0
    ASSERT_EQ(parser.ParseVariant16Tag(), 0);

    // row 1
    ASSERT_EQ(parser.ParseVariant16Tag(), 1);
    ASSERT_EQ(parser.ParseVariant8Tag(), 0);
    ASSERT_EQ(parser.ParseUint64(), 1);

    // row 2
    ASSERT_EQ(parser.ParseVariant16Tag(), 1);
    ASSERT_EQ(parser.ParseVariant8Tag(), 1);
    ASSERT_EQ(parser.ParseInt64(), 2);
    ASSERT_EQ(parser.ParseUint64(), 3);

    // end
    ASSERT_EQ(parser.ParseVariant16Tag(), EndOfSequenceTag<ui16>());

    parser.ValidateFinished();
}

////////////////////////////////////////////////////////////////////////////////
