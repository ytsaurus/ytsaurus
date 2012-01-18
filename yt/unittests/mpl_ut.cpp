#include "stdafx.h"

#include "../ytlib/misc/mpl.h"

#include <contrib/testing/framework.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TSomeStruct { };
class TSomeClass { };
enum TSomeEnum { };

class TParent
{ };

class TChild : public TParent
{ };

template<class T, typename = void>
class TWrapped
{
public:
    int CleverTemplateFunction()
    {
        return 17;
    }
};

template<class T>
class TWrapped<T, typename NMpl::TEnableIf<
        NMpl::TIsConvertible<T*, TParent*>
    >::TType>
{
public:
    int CleverTemplateFunction()
    {
        return 42;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST(MetaProgrammingTest, IsConvertible)
{
    EXPECT_TRUE ( (NMpl::TIsConvertible<TChild, TParent>::Value) );
    EXPECT_FALSE( (NMpl::TIsConvertible<TParent, TChild>::Value) );

    EXPECT_TRUE ( (NMpl::TIsConvertible<TChild*, TParent*>::Value) );
    EXPECT_FALSE( (NMpl::TIsConvertible<TParent*, TChild*>::Value) );
    EXPECT_TRUE ( (NMpl::TIsConvertible<TChild*, TChild*>::Value) );
    EXPECT_TRUE ( (NMpl::TIsConvertible<TParent*, TParent*>::Value) );

    EXPECT_FALSE( (NMpl::TIsConvertible<TParent, TSomeStruct>::Value) );
    EXPECT_FALSE( (NMpl::TIsConvertible<TParent, TSomeClass>::Value) );
    EXPECT_FALSE( (NMpl::TIsConvertible<TParent, TSomeEnum>::Value) );

    EXPECT_TRUE ( (NMpl::TIsConvertible<int, float>::Value) );
    EXPECT_TRUE ( (NMpl::TIsConvertible<int, double>::Value) );

    EXPECT_TRUE ( (NMpl::TIsConvertible<int*, void*>::Value) );
    EXPECT_FALSE( (NMpl::TIsConvertible<void*, int*>::Value) );

    EXPECT_FALSE( (NMpl::TIsConvertible<int[17], double>::Value) );
    EXPECT_FALSE( (NMpl::TIsConvertible<double, int[17]>::Value) );
    EXPECT_TRUE ( (NMpl::TIsConvertible<int[17], int*>::Value) );
}

TEST(MetaProgrammingTest, EnableIf)
{
    EXPECT_EQ(17, TWrapped<int>().CleverTemplateFunction());
    EXPECT_EQ(17, TWrapped<float>().CleverTemplateFunction());
    EXPECT_EQ(42, TWrapped<TParent>().CleverTemplateFunction());
    EXPECT_EQ(42, TWrapped<TChild>().CleverTemplateFunction());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace <anonymous>
} // namespace NYT
