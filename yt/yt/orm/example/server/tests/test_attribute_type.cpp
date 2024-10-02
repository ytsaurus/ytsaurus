#include "common.h"

#include <yt/yt/orm/server/objects/object_manager.h>
#include <yt/yt/orm/server/objects/transaction.h>
#include <yt/yt/orm/server/objects/transaction_manager.h>

namespace NYT::NOrm::NExample::NServer::NTests {

using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TAttributeTypeTest, Enum)
{
    auto typeHandler = GetBootstrap()->GetObjectManager()
        ->GetTypeHandlerOrCrash(TObjectTypeValues::Cat);

    auto schema = typeHandler->GetRootAttributeSchema()
        ->FindChild("spec")
        ->AsComposite()
        ->FindChild("health_condition")
        ->AsScalar();
    ASSERT_EQ(schema->GetType(), EAttributeType::String);

    schema = typeHandler->GetRootAttributeSchema()
        ->FindChild("spec")
        ->AsComposite()
        ->FindChild("mood")
        ->AsScalar();
    ASSERT_EQ(schema->GetType(), EAttributeType::Int64);
}

TEST(TAttributeTypeTest, List)
{
    auto typeHandler = GetBootstrap()->GetObjectManager()
        ->GetTypeHandlerOrCrash(TObjectTypeValues::Book);

    auto schema = typeHandler->GetRootAttributeSchema()
        ->FindChild("spec")
        ->AsComposite()
        ->FindChild("genres")
        ->AsScalar();
    ASSERT_EQ(schema->GetType(), EAttributeType::List);
}

TEST(TAttributeTypeTest, Map)
{
    auto typeHandler = GetBootstrap()->GetObjectManager()
        ->GetTypeHandlerOrCrash(TObjectTypeValues::Book);

    auto schema = typeHandler->GetRootAttributeSchema()
        ->FindChild("meta")
        ->AsComposite()
        ->FindChild("finalizers")
        ->AsScalar();
    ASSERT_EQ(schema->GetType(), EAttributeType::Map);
}

TEST(TAttributeTypeTest, Scalar)
{
    auto typeHandler = GetBootstrap()->GetObjectManager()
        ->GetTypeHandlerOrCrash(TObjectTypeValues::Book);

    auto schema = typeHandler->GetRootAttributeSchema()
        ->FindChild("spec")
        ->AsComposite()
        ->FindChild("name")
        ->AsScalar();
    ASSERT_EQ(schema->GetType(), EAttributeType::String);

    schema = typeHandler->GetRootAttributeSchema()
        ->FindChild("meta")
        ->AsComposite()
        ->FindChild("inherit_acl")
        ->AsScalar();
    ASSERT_EQ(schema->GetType(), EAttributeType::Boolean);

    schema = typeHandler->GetRootAttributeSchema()
        ->FindChild("meta")
        ->AsComposite()
        ->FindChild("ultimate_question_of_life")
        ->AsScalar();
    ASSERT_EQ(schema->GetType(), EAttributeType::Uint64);
}

TEST(TAttributeTypeTest, Message)
{
    auto typeHandler = GetBootstrap()->GetObjectManager()
        ->GetTypeHandlerOrCrash(TObjectTypeValues::Book);

    auto schema = typeHandler->GetRootAttributeSchema()
        ->FindChild("spec")
        ->AsComposite()
        ->FindChild("digital_data")
        ->AsScalar();

    ASSERT_EQ(schema->GetType(), EAttributeType::Message);
    ASSERT_EQ(schema->GetType("/available_formats"), EAttributeType::List);
    ASSERT_EQ(schema->GetType("/available_formats/2"), EAttributeType::Message);
    ASSERT_EQ(schema->GetType("/available_formats/2/format"), EAttributeType::Int64);
}

TEST(TAttributeTypeTest, Etc)
{
    auto typeHandler = GetBootstrap()->GetObjectManager()
        ->GetTypeHandlerOrCrash(TObjectTypeValues::NestedColumns);

    auto schema = typeHandler->GetRootAttributeSchema()
        ->FindChild("spec")
        ->AsComposite()
        ->GetEtcChild();

    ASSERT_EQ(schema->GetType(), EAttributeType::Message);
    ASSERT_EQ(schema->GetType("/composite_map"), EAttributeType::Map);
    ASSERT_EQ(schema->GetType("/composite_map/some_string"), EAttributeType::Message);
    ASSERT_EQ(schema->GetType("/composite_map/some_string/direct_repeated"), EAttributeType::List);
    ASSERT_EQ(schema->GetType("/composite_map/some_string/direct_repeated/0"), EAttributeType::Message);
    ASSERT_EQ(schema->GetType("/composite_map/some_string/direct_repeated/0/foo"), EAttributeType::String);
}

TEST(TAttributeTypeTest, Nested)
{
    auto typeHandler = GetBootstrap()->GetObjectManager()
        ->GetTypeHandlerOrCrash(TObjectTypeValues::Genre);

    auto schema = typeHandler->GetRootAttributeSchema()
        ->FindChild("spec")
        ->AsComposite()
        ->FindChild("is_name_substring")
        ->AsScalar();
    ASSERT_EQ(schema->GetType("/some_string"), EAttributeType::Boolean);
    EXPECT_THROW_WITH_SUBSTRING(
        schema->GetType("/some_string/"),
        "Can not resolve path");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NTests
