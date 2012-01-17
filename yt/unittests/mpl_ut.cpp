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
class TWrapped<T, typename NMPL::TEnableIf<
        NMPL::TIsConvertible<T*, TParent*>
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
    EXPECT_TRUE ( (NMPL::TIsConvertible<TChild, TParent>::Value) );
    EXPECT_FALSE( (NMPL::TIsConvertible<TParent, TChild>::Value) );

    EXPECT_TRUE ( (NMPL::TIsConvertible<TChild*, TParent*>::Value) );
    EXPECT_FALSE( (NMPL::TIsConvertible<TParent*, TChild*>::Value) );
    EXPECT_TRUE ( (NMPL::TIsConvertible<TChild*, TChild*>::Value) );
    EXPECT_TRUE ( (NMPL::TIsConvertible<TParent*, TParent*>::Value) );

    EXPECT_FALSE( (NMPL::TIsConvertible<TParent, TSomeStruct>::Value) );
    EXPECT_FALSE( (NMPL::TIsConvertible<TParent, TSomeClass>::Value) );
    EXPECT_FALSE( (NMPL::TIsConvertible<TParent, TSomeEnum>::Value) );

    EXPECT_TRUE ( (NMPL::TIsConvertible<int, float>::Value) );
    EXPECT_TRUE ( (NMPL::TIsConvertible<int, double>::Value) );

    EXPECT_TRUE ( (NMPL::TIsConvertible<int*, void*>::Value) );
    EXPECT_FALSE( (NMPL::TIsConvertible<void*, int*>::Value) );

    EXPECT_FALSE( (NMPL::TIsConvertible<int[17], double>::Value) );
    EXPECT_FALSE( (NMPL::TIsConvertible<double, int[17]>::Value) );
    EXPECT_TRUE ( (NMPL::TIsConvertible<int[17], int*>::Value) );
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
