//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  std::scoped_lock<std::mutex> bpm_lock(latch_);
  // 1.     Search the page table for the requested page (P).
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
      // 1.1    If P exists, pin it and return it immediately.
     frame_id_t frame_id = it->second;
     Page* page = pages_ + frame_id;
     page->pin_count_++;
     replacer_->Pin(frame_id);
    return page;
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  frame_id_t frame_id = findReplaceFrame();
  if (-1 == frame_id) {
    return nullptr;
  }
  Page * page = pages_ + frame_id;
  // 3.     Delete R from the page table and insert P.
  page_table_[page_id] = frame_id;

  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  page->page_id_ = page_id;
  page->ResetMemory();
  disk_manager_->ReadPage(page_id, page->GetData());
  page->pin_count_++;
  return page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::scoped_lock<std::mutex> bpm_lock{latch_};
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  auto frame_id = it->second;
  Page* p = pages_ + frame_id;
  if (p->pin_count_ <= 0) {
    return false;
  }
  p->pin_count_--;
  if (0 == p->pin_count_) {
    replacer_->Unpin(frame_id);
  }
  p->is_dirty_ |= is_dirty;
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::scoped_lock<std::mutex> bpm_lock{latch_};
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  auto frame_id = it->second;
  Page* p = pages_ + frame_id;
  disk_manager_->WritePage(p->page_id_, p->GetData());
  p->is_dirty_ = false;

  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  if (IsAllPinned()) {
    return nullptr;
  }
  *page_id = disk_manager_->AllocatePage();
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t frame_id = findReplaceFrame();
  if (-1 == frame_id) {
    return nullptr;
  }
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  Page * page = pages_ + frame_id;
  InitPage(*page_id, page);
  page_table_[*page_id] = frame_id;
  // 4.   Set the page ID output parameter. Return a pointer to P.
  return page;
}

void BufferPoolManager::InitPage(const page_id_t page_id, Page *page) const {
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page->ResetMemory();
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::scoped_lock<std::mutex> bpm_lock{latch_};
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }
  auto frame_id = it->second;
  Page* p = pages_ + frame_id;
  if (0 < p->pin_count_) {
    return false;
  }
  // 0 == pin_count
  page_table_.erase(p->GetPageId());
  disk_manager_->DeallocatePage(p->page_id_);
  replacer_->Pin(frame_id);

  p->page_id_ = INVALID_PAGE_ID;
  p->is_dirty_ = false;
  p->ResetMemory();

  free_list_.push_front(frame_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  std::scoped_lock<std::mutex> bpm_lock{latch_};
  for (int i = 0; i < static_cast<int>(pool_size_); i++) {
    Page* p = pages_ + i;
    disk_manager_->WritePage(p->page_id_, p->GetData());
    p->is_dirty_ = false;
  }
}

bool BufferPoolManager::IsAllPinned() {
  return free_list_.empty() && replacer_->Size() == 0;
}

frame_id_t BufferPoolManager::findReplaceFrame() {
  Page * page = nullptr;
  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {
    //        Note that pages are always found from the free list first.
    frame_id = free_list_.back();
    free_list_.pop_back();
    assert(0 <= frame_id && frame_id < static_cast<frame_id_t >(pool_size_));
    page = pages_ + frame_id;
  } else {
    bool victimized= replacer_->Victim(&frame_id);
    if (!victimized) {
      return -1;
    }
    page = pages_ + frame_id;
    if (page->is_dirty_) {
      // 2.     If R is dirty, write it back to the disk.
      disk_manager_->WritePage(page->page_id_, page->GetData());
      page->is_dirty_ = false;
    }
    page->pin_count_ = 0;
  }
  page_table_.erase(page->GetPageId());
  return frame_id;
}

}  // namespace bustub
