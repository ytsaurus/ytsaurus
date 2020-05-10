#include "checked_parser.h"
#include "checked_writer.h"
#include "skiff_schema.h"
#include "unchecked_parser.h"
#include "unchecked_writer.h"

#include <library/cpp/unittest/registar.h>

#include <util/stream/buffer.h>

using namespace NSkiff;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSkiffTestSuite) {
    Y_UNIT_TEST(TestInt)
    {
        TBufferStream bufferStream;

        auto schema = CreateSimpleTypeSchema(EWireType::Uint64);

        TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
        tokenWriter.WriteUint64(42);
        tokenWriter.WriteUint64(100500);

        tokenWriter.Finish();

        TCheckedSkiffParser parser(schema, &bufferStream);

        UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint64(), 42);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint64(), 100500);
    }

    Y_UNIT_TEST(TestBoolean)
    {
        auto schema = CreateSimpleTypeSchema(EWireType::Boolean);

        TBufferStream bufferStream;
        TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
        tokenWriter.WriteBoolean(true);
        tokenWriter.WriteBoolean(false);
        tokenWriter.Finish();

        TCheckedSkiffParser parser(schema, &bufferStream);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseBoolean(), true);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseBoolean(), false);

        {
            TBufferStream bufferStream;
            bufferStream.Write('\x02');

            TCheckedSkiffParser parser(schema, &bufferStream);
            UNIT_ASSERT_EXCEPTION(parser.ParseBoolean(), yexception);
        }
    }

    Y_UNIT_TEST(TestVariant8)
    {
        auto schema = CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Uint64),
        });

        {
            TBufferStream bufferStream;
            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
            UNIT_ASSERT_EXCEPTION(tokenWriter.WriteUint64(42), yexception);
        }

        {
            TBufferStream bufferStream;
            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
            tokenWriter.WriteVariant8Tag(0);
            UNIT_ASSERT_EXCEPTION(tokenWriter.WriteUint64(42), yexception);
        }
        {
            TBufferStream bufferStream;
            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
            tokenWriter.WriteVariant8Tag(1);
            UNIT_ASSERT_EXCEPTION(tokenWriter.WriteInt64(42), yexception);
        }

        TBufferStream bufferStream;
        TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
        tokenWriter.WriteVariant8Tag(0);
        tokenWriter.WriteVariant8Tag(1);
        tokenWriter.WriteUint64(42);
        tokenWriter.Finish();

        TCheckedSkiffParser parser(schema, &bufferStream);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseVariant8Tag(), 0);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseVariant8Tag(), 1);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint64(), 42);

        parser.ValidateFinished();
    }

    Y_UNIT_TEST(TestTuple)
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
            UNIT_ASSERT_VALUES_EQUAL(parser.ParseInt64(), 42);
            UNIT_ASSERT_VALUES_EQUAL(parser.ParseString32(), "foobar");
            parser.ValidateFinished();
        }
    }

    Y_UNIT_TEST(TestString)
    {

        auto schema = CreateSimpleTypeSchema(EWireType::String32);

        {
            TBufferStream bufferStream;

            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
            tokenWriter.WriteString32("foo");
            tokenWriter.Finish();

            TCheckedSkiffParser parser(schema, &bufferStream);

            UNIT_ASSERT_VALUES_EQUAL(parser.ParseString32(), "foo");

            parser.ValidateFinished();
        }

        {
            TBufferStream bufferStream;

            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);
            tokenWriter.WriteString32("foo");
            tokenWriter.Finish();

            TCheckedSkiffParser parser(schema, &bufferStream);
            UNIT_ASSERT_EXCEPTION(parser.ParseInt64(), yexception);
        }
    }

    Y_UNIT_TEST(TestRepeatedVariant16)
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
            UNIT_ASSERT_VALUES_EQUAL(parser.ParseVariant16Tag(), 0);
            UNIT_ASSERT_VALUES_EQUAL(parser.ParseInt64(), -8);

            // row 2
            UNIT_ASSERT_VALUES_EQUAL(parser.ParseVariant16Tag(), 1);
            UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint64(), 42);

            // end
            UNIT_ASSERT_VALUES_EQUAL(parser.ParseVariant16Tag(), EndOfSequenceTag<ui16>());

            parser.ValidateFinished();
        }

        {
            TBufferStream bufferStream;
            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);

            tokenWriter.WriteVariant16Tag(0);
            UNIT_ASSERT_EXCEPTION(tokenWriter.WriteUint64(5), yexception);
        }

        {
            TBufferStream bufferStream;
            TCheckedSkiffWriter tokenWriter(schema, &bufferStream);

            tokenWriter.WriteVariant16Tag(1);
            tokenWriter.WriteUint64(5);

            UNIT_ASSERT_EXCEPTION(tokenWriter.Finish(), yexception);
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

                UNIT_ASSERT_EXCEPTION(parser.ParseInt64(), yexception);
            }

            {
                TBufferInput input(bufferStream.Buffer());
                TCheckedSkiffParser parser(schema, &input);

                parser.ParseVariant16Tag();
                UNIT_ASSERT_EXCEPTION(parser.ParseUint64(), yexception);
            }

            {
                TBufferInput input(bufferStream.Buffer());
                TCheckedSkiffParser parser(schema, &input);

                parser.ParseVariant16Tag();
                parser.ParseInt64();

                UNIT_ASSERT_EXCEPTION(parser.ValidateFinished(), yexception);
            }
        }
    }

    Y_UNIT_TEST(TestStruct)
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
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseVariant16Tag(), 0);

        // row 1
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseVariant16Tag(), 1);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseVariant8Tag(), 0);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint64(), 1);

        // row 2
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseVariant16Tag(), 1);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseVariant8Tag(), 1);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseInt64(), 2);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseUint64(), 3);

        // end
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseVariant16Tag(), EndOfSequenceTag<ui16>());

        parser.ValidateFinished();
    }
};

////////////////////////////////////////////////////////////////////////////////
