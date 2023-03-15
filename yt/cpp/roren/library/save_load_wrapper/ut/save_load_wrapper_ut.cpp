#include <yt/cpp/roren/library/save_load_wrapper/interprocess_test/test/virtual.h>
#include <yt/cpp/roren/library/save_load_wrapper/save_load_wrapper.h>

#include <memory>

#include <library/cpp/testing/gtest/gtest.h>
#include <util/stream/str.h>

using namespace NRoren;

TEST(SaveLoadHolder, Simple) {
    TStringStream s;

    {
        ::SaveMany(&s, TVirtualPtr{}, 42);
        TVirtualPtr v;
        int x = 0;
        ::LoadMany(&s, v, x);
        EXPECT_EQ(v, nullptr);
        EXPECT_EQ(x, 42);
    }

    TVirtualPtr v = std::make_shared<TImpl1>(100);
    ::Save(&s, v);
    v.reset();
    ::Load(&s, v);
    EXPECT_EQ(v->GetData(), 101);

    TVirtualPtr x = std::make_shared<TImpl2>(200);
    ::SaveMany(&s, v, x);
    v.reset();
    x.reset();
    ::LoadMany(&s, x, v);
    EXPECT_EQ(x->GetData(), 101);
    EXPECT_EQ(v->GetData(), 202);
}

