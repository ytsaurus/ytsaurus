#include <yt/yt/library/web_assembly/api/compartment.h>
#include <yt/yt/library/web_assembly/api/function.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NWebAssembly {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TWebAssemblyTest
    : public ::testing::Test
{ };

TEST_F(TWebAssemblyTest, Create)
{
    std::vector<std::unique_ptr<IWebAssemblyCompartment>> compartments;
    for (int i = 0; i < 1024; ++i) {
        compartments.push_back(CreateBaseImage());
    }
}

TEST_F(TWebAssemblyTest, AllocateAndFree)
{
    auto compartment = CreateBaseImage();
    std::vector<uintptr_t> offsets;
    for (int i = 0; i < 1024; ++i) {
        auto offset = compartment->AllocateBytes(1024);
        offsets.push_back(offset);
    }

    for (auto offset : offsets) {
        compartment->FreeBytes(offset);
    }
}

static const TString AddAndMul = R"(
    (module
        (type (;0;) (func (param i64 i64) (result i64)))

        (func $add (type 0) (param $first i64) (param $second i64) (result i64)
            (i64.add
                (local.get $first)
                (local.get $second)
            )
        )

        (func $mul (type 0) (param $0 i64) (param $1 i64) (result i64)
            (local.get $1)
            (local.get $0)
            (i64.mul)
        )

        (export "add" (func $add))
        (export "mul" (func $mul))
    ))";

TEST_F(TWebAssemblyTest, LinkAndStrip)
{
    auto compartment = CreateBaseImage();
    compartment->AddModule(AddAndMul);
    compartment->Strip();
}

TEST_F(TWebAssemblyTest, RunSimple)
{
    auto compartment = CreateBaseImage();
    compartment->AddModule(AddAndMul);

    auto add = TCompartmentFunction<i64(i64, i64)>(compartment.get(), "add");
    for (int i = 0; i < 1024; ++i) {
        i64 a = std::rand();
        i64 b = std::rand();
        auto result = add(a, b);
        ASSERT_EQ(result, a + b);
    }

    auto mul = TCompartmentFunction<i64(i64, i64)>(compartment.get(), "mul");
    for (int i = 0; i < 1024; ++i) {
        i64 a = std::rand();
        i64 b = std::rand();
        auto result = mul(a, b);
        ASSERT_EQ(result, a * b);
    }
}

TEST_F(TWebAssemblyTest, Clone)
{
    auto compartment = CreateBaseImage();
    compartment->AddModule(AddAndMul);

    for (int i = 0; i < 1024; ++i) {
        compartment = compartment->Clone();
    }

    auto add = TCompartmentFunction<i64(i64, i64)>(compartment.get(), "add");
    for (int i = 0; i < 1024; ++i) {
        i64 a = std::rand();
        i64 b = std::rand();
        auto result = add(a, b);
        ASSERT_EQ(result, a + b);
    }

    auto mul = TCompartmentFunction<i64(i64, i64)>(compartment.get(), "mul");
    for (int i = 0; i < 1024; ++i) {
        i64 a = std::rand();
        i64 b = std::rand();
        auto result = mul(a, b);
        ASSERT_EQ(result, a * b);
    }
}

static const TString ArraySum = R"(
    (module
        (import "env" "memory" (memory i64 8 2097152))

        (type (;0;) (func (param i64 i64) (result i64)))

        (func $sum (type 0) (param $ptr i64) (param $length i64) (result i64)
            (local $result i64)
            (local $index i64)

            (local.set $result (i64.const 0))
            (local.set $index (i64.const 0))

            (block $IF
                (br_if $IF (i64.ge_s (local.get $index) (local.get $length)))

                (loop $LOOP
                    (local.set $result (i64.add (local.get $result) (i64.load (local.get $ptr))))

                    (local.set $ptr (i64.add (local.get $ptr) (i64.const 8)))

                    (local.set $index (i64.add (local.get $index) (i64.const 1)))

                    (br_if $LOOP (i64.ne (local.get $index) (local.get $length)))
                )
            )

            (local.get $result)
        )

        (export "sum" (func $sum))
    ))";

TEST_F(TWebAssemblyTest, SimpleArraySum)
{
    auto compartment = CreateBaseImage();
    compartment->AddModule(ArraySum);
    auto sum = TCompartmentFunction<i64(i64, i64)>(compartment.get(), "sum");

    i64 maxLength = 42;

    for (int iteration = 0; iteration < 100; ++iteration) {
        i64 length = std::rand() % maxLength;
        auto byteLength = sizeof(ui64) * length;
        auto offset = compartment->AllocateBytes(byteLength);
        auto hostPointer = compartment->GetHostPointer(offset, byteLength);
        i64* array = std::bit_cast<i64*>(hostPointer);

        i64 expected = 0;
        for (i64 i = 0; i < length; ++i) {
            array[i] = std::rand() % 10;
            expected += array[i];
        }

        auto actual = sum(offset, length);
        ASSERT_EQ(actual, expected);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NWebAssembly
