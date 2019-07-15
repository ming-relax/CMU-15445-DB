#include <include/common/logger.h>
#include "buffer/buffer_pool_manager.h"

namespace cmudb {

/*
 * BufferPoolManager Constructor
 * When log_manager is nullptr, logging is disabled (for test purpose)
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::BufferPoolManager(size_t pool_size,
                                                 DiskManager *disk_manager,
                                                 LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager),
      log_manager_(log_manager) {
  // a consecutive memory space for buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHash<page_id_t, Page *>(BUCKET_SIZE);
  replacer_ = new LRUReplacer<Page *>;
  free_list_ = new std::list<Page *>;

  // put all the pages into free list
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_->push_back(&pages_[i]);
  }
}

/*
 * BufferPoolManager Deconstructor
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
  delete free_list_;
}

/**
 * 1. search hash table.
 *  1.1 if exist, pin the page and return immediately
 *  1.2 if no exist, find a replacement entry from either free list or lru
 *      replacer. (NOTE: always find from free list first)
 * 2. If the entry chosen for replacement is dirty, write it back to disk.
 * 3. Delete the entry for the old page from the hash table and insert an
 * entry for the new page.
 * 4. Update page metadata, read page content from disk file and return page
 * pointer
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  Page *ret_page = nullptr;

  latch_.lock();
  auto found = page_table_->Find(page_id, ret_page);
  if (found) {
    // found the page in hash table
    ret_page->pin_count_ += 1;
  } else {
    // page is not in hash table, we need to bring it from disk manager
    ret_page = getFreePage();
    if (nullptr != ret_page) {
      initAndPinPage(ret_page, page_id);
      disk_manager_->ReadPage(page_id, ret_page->data_);
    }
  }
  latch_.unlock();
  return ret_page;
}

/*
 * Implementation of unpin page
 * if pin_count>0, decrement it and if it becomes zero, put it back to
 * replacer if pin_count<=0 before this call, return false. is_dirty: set the
 * dirty flag of this page
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  bool b_ret = true;
  bool found;
  Page *page;

  latch_.lock();
  found = page_table_->Find(page_id, page);
  if (found) {
    if (page->pin_count_ > 0) {
      page->pin_count_--;
      if (page->pin_count_ == 0) {
        replacer_->Insert(page);
      }
      b_ret = true;
    } else {
      b_ret = false;
    }

    if (is_dirty) {
      page->is_dirty_ = true;
    }
  } else {
    b_ret = false;
  }
  latch_.unlock();
  return b_ret;
}

/*
 * Used to flush a particular page of the buffer pool to disk. Should call the
 * write_page method of the disk manager
 * if page is not found in page table, return false
 * NOTE: make sure page_id != INVALID_PAGE_ID
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  bool b_ret;
  bool found;
  Page *page;

  latch_.lock();
  found = page_table_->Find(page_id, page);
  if (found) {
    disk_manager_->WritePage(page_id, page->data_);
    page->is_dirty_ = false;
    b_ret = true;
  } else {
    b_ret = false;
  }
  latch_.unlock();
  return b_ret;
}

/**
 * User should call this method for deleting a page. This routine will call
 * disk manager to deallocate the page. First, if page is found within page
 * table, buffer pool manager should be reponsible for removing this entry out
 * of page table, reseting page metadata and adding back to free list. Second,
 * call disk manager's DeallocatePage() method to delete from disk file. If
 * the page is found within page table, but pin_count != 0, return false
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  bool b_ret = false;
  bool found;
  Page *page = nullptr;

  found = page_table_->Find(page_id, page);
  if (found) {
    if (page->pin_count_ > 0) {
      b_ret = false;
    } else {
      // pin count == 0, page must be in lru_replacer
      if (false == (page_table_->Remove(page_id))) {
        LOG_ERROR("DeletePage page_id is not in page_table_");
      } else {
        if (false == replacer_->Erase(page)) {
          LOG_ERROR("DeletePage page is not in replacer_");
        } else {
          page->ResetMemory();
          page->page_id_ = INVALID_PAGE_ID;
          page->pin_count_ = 0;
          page->is_dirty_ = false;
          free_list_->push_back(page);
          b_ret = true;
        }
      }
    }
  } else {
    // page not found int page_table_
    b_ret = false;
  }

  if (b_ret) {
    disk_manager_->DeallocatePage(page_id);
  }

  return b_ret;
}

/**
 * User should call this method if needs to create a new page. This routine
 * will call disk manager to allocate a page.
 * Buffer pool manager should be responsible to choose a victim page either
 * from free list or lru replacer(NOTE: always choose from free list first),
 * update new page's metadata, zero out memory and add corresponding entry
 * into page table. return nullptr if all the pages in pool are pinned
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  Page *ret_page = nullptr;

  latch_.lock();
  page_id = disk_manager_->AllocatePage();
  // find a page object
  ret_page = getFreePage();
  if (nullptr != ret_page) {
    initAndPinPage(ret_page, page_id);
  }
  latch_.unlock();
  return ret_page;
}


Page *BufferPoolManager::getFreePage() {
  Page *ret_page = nullptr;
  if (free_list_->size() > 0) {
    ret_page = free_list_->front();
    free_list_->pop_front();
  } else {
    auto ok = replacer_->Victim(ret_page);
    if (ok) {
      // find a victim
      if (ret_page->is_dirty_) {
        disk_manager_->WritePage(ret_page->page_id_, ret_page->data_);
      }
    } else {
      // no victim found, ret_page is nullptr
    }
  }
  return ret_page;
}

void BufferPoolManager::initAndPinPage(Page *page, page_id_t new_page_id) {
  if (nullptr != page) {
    page_table_->Remove(page->page_id_);
    page->ResetMemory();
    page->page_id_ = new_page_id;
    page->pin_count_ = 1;
    page->is_dirty_ = false;
    page_table_->Insert(new_page_id, page);
  }
}

} // namespace cmudb
