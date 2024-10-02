#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/orm/server/objects/build_tags.h>

using namespace NYT::NOrm::NClient::NObjects;

namespace NYT::NOrm::NServer::NObjects::NTests {

////////////////////////////////////////////////////////////////////////////////

TEST(TBuildTagsTest, Recursive)
{
    TTagsTreeNode tree{
        .Children = {
            {
                "a",
                {
                    .Children = {
                        {
                            "b",
                            {
                                .AddTags = {8},
                                .RemoveTags = {4},
                            }
                        },
                        {
                            "c",
                            {
                                .AddTags = {16},
                            }
                        },
                    },
                    .MessageTags = {2},
                    .AddTags = {4},
                }
            },
        },
        .AddTags = {1},
    };

    TTagsTreeNode expected{
        .Children = {
            {
                "a",
                {
                    .Children = {
                        {
                            "b",
                            {
                                .Tags = {1, 2, 8},
                            }
                        },
                        {
                            "c",
                            {
                                .Tags = {1, 2, 4, 16},
                            }
                        },
                    },
                    .Tags = {1, 2, 4},
                }
            },
        },
        .Tags = {1},
    };

    NDetail::BuildTagsRecursively(tree);
    EXPECT_EQ(tree, expected);
}

TEST(TBuildTagsTest, RecursiveNothingToCollapse)
{
    TTagsTreeNode tree{
        .Children = {
            {
                "a",
                {
                    .Children = {
                        {
                            "b",
                            {
                                .Tags = {1, 2, 8},
                            }
                        },
                        {
                            "c",
                            {
                                .Tags = {1, 2, 4, 16},
                            }
                        },
                    },
                    .Tags = {1, 2, 4},
                }
            },
            {
                "b",
                {
                    .Tags = {4, 8},
                }
            }
        },
        .Tags = {1},
    };

    TTagsTreeNode expected{tree};
    NDetail::CollapseTagsRecursively(tree);
    EXPECT_EQ(tree, expected);
}

TEST(TBuildTagsTest, RecursiveCollapse)
{
    TTagsTreeNode tree{
        .Children = {
            {
                "a",
                {
                    .Children = {
                        {
                            "b",
                            {
                                .Tags = {1, 2, 4},
                            }
                        },
                        {
                            "c",
                            {
                                .Tags = {1, 2, 4},
                            }
                        },
                    },
                    .Tags = {1, 2, 4},
                }
            },
            {
                "b",
                {
                    .Tags = {1, 2, 4},
                }
            },
        },
        .Tags = {1},
    };

    TTagsTreeNode expected{
        .Children = {
            {
                "a",
                {
                    .Tags = {1, 2, 4},
                }
            },
            {
                "b",
                {
                    .Tags = {1, 2, 4},
                }
            },
        },
        .Tags = {1},
    };

    NDetail::CollapseTagsRecursively(tree);
    EXPECT_EQ(tree, expected);
}

TEST(TBuildTagsIndexTest, All)
{

    TTagsTreeNode tree{
        .Children = {
            {
                "a",
                {
                    .Children = {
                        {
                            "b",
                            {
                                .Tags = {1, 4, 8},
                            }
                        },
                        {
                            "c",
                            {
                                .Tags = {1, 2, 4, 16},
                            }
                        },
                    },
                    .Tags = {1, 2, 4},
                }
            },
            {
                "b",
                {
                    .Children = {
                        {
                            "e",
                            {
                                .Tags = {4, 8, 16}
                            }
                        },
                        {
                            "f",
                            {
                                .Tags = {4, 32}
                            }
                        }
                    },
                    .Repeated = true,
                    .Tags = {4, 8},
                }
            }
        },
        .Tags = {1},
    };

    std::vector<TTagsIndexEntry> expected{
        {.Path="/a/b", .Tags = {1, 4, 8}},
        {.Path="/a/c", .Tags = {1, 2, 4, 16}},
        {.Path="/b/*/f", .Tags = {4, 32}},
        {.Path="/b/*/e", .Tags = {4, 8, 16}},
    };
    auto index = NDetail::BuildTagsIndex(tree, NDetail::ETagsIndexType::All);
    EXPECT_THAT(index, testing::ElementsAreArray(expected));
}

TEST(TBuildTagsIndexTest, Etc)
{

    TTagsTreeNode tree{
        .Children = {
            {
                "a",
                {
                    .Children = {
                        {
                            "b",
                            {
                                .Etc = true,
                                .Tags = {1, 4, 8},
                            }
                        },
                        {
                            "c",
                            {
                                .Tags = {1, 2, 4, 16},
                            }
                        },
                    },
                    .Etc = true,
                    .Tags = {1, 2, 4},
                }
            },
            {
                "b",
                {
                    .Children = {
                        {
                            "e",
                            {
                                .Etc = true,
                                .Tags = {4, 8, 16}
                            }
                        },
                        {
                            "f",
                            {
                                .Tags = {4, 32}
                            }
                        }
                    },
                    .Etc = true,
                    .Repeated = true,
                    .Tags = {4, 8},
                }
            },
            {
                "g",
                {
                    .Tags = {16, 64},
                }
            },
            {
                "h",
                {
                    .Etc = true,
                    .Tags = {16, 64},
                }
            },
            {
                "i",
                {
                    .EtcName = "search",
                    .Etc = true,
                    .Tags = {16, 128},
                }
            }
        },
        .Tags = {1},
    };

    std::vector<TTagsIndexEntry> expected{
        {.Path="/a/b", .Tags = {1, 4, 8}},
        {.Path="/b/*/e", .Tags = {4, 8, 16}},
        {.Path="/h", .Tags = {16, 64}},
    };
    auto index = NDetail::BuildTagsIndex(tree, NDetail::ETagsIndexType::EtcOnly);
    EXPECT_THAT(index, testing::ElementsAreArray(expected));

    std::vector<TTagsIndexEntry> expectedSearch{
        {.Path="/i", .Tags = {16, 128}},
    };
    auto indexSearch = NDetail::BuildTagsIndex(tree, NDetail::ETagsIndexType::EtcOnly, "search");
    EXPECT_THAT(indexSearch, testing::ElementsAreArray(expectedSearch));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects::NTests
