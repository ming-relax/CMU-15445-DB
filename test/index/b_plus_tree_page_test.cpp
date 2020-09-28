#include "page/b_plus_tree_page.h"
#include "page/b_plus_tree_internal_page.h"
#include "page/b_plus_tree_leaf_page.h"
#include "common/logger.h"

#include "gtest/gtest.h"

// huming

namespace cmudb {

TEST(BPlusTreePageTests, InternalPage) {
  // Given a KeyType, we should have a valid size
  // For PAGE_SIZE = 512
  // Header = 24
  // (k, v) = 4 + 4 = 8 bytes
  // num of kv pairs = (512 - 24) / 8 = 61
  BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>> internal_page;
  internal_page.Init(2, 3);
  EXPECT_EQ(internal_page.IsLeafPage(), false);
  EXPECT_EQ(internal_page.IsRootPage(), false);
  EXPECT_EQ(internal_page.GetMaxSize(), 61);
  EXPECT_EQ(internal_page.GetSize(), 0);
  EXPECT_EQ(internal_page.GetMinSize(), 30);

  // (512 - 24) / 12 =  40.6
  BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>> leaf_page;
  leaf_page.Init(2, 3);
  EXPECT_EQ(leaf_page.IsLeafPage(), true);
  EXPECT_EQ(leaf_page.IsRootPage(), false);
  EXPECT_EQ(leaf_page.GetMaxSize(), 40);
  EXPECT_EQ(leaf_page.GetSize(), 0);

}

}