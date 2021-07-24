/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t cur_page_id, int idx, BufferPoolManager* buffer_pool_manager)  {
    cur_page_id_ = cur_page_id;
    idx_ = idx;
    buffer_pool_manager_ = buffer_pool_manager;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {
    auto page = buffer_pool_manager_->FetchPage(cur_page_id_);
    if (nullptr == page) {
        throw "illegal state";
    }
    auto leaf_page = reinterpret_cast<LeafPage*>(page->GetData());
    return INVALID_PAGE_ID == leaf_page->GetNextPageId() && idx_ == leaf_page->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
    auto page = buffer_pool_manager_->FetchPage(cur_page_id_);
    if (nullptr == page) {
        throw "illegal state";
    }
    auto leaf_page = reinterpret_cast<LeafPage*>(page->GetData());
    return leaf_page->GetItem(idx_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
    auto page = buffer_pool_manager_->FetchPage(cur_page_id_);
    if (nullptr == page) {
        throw "illegal state";
    }

    auto leaf_page = reinterpret_cast<LeafPage*>(page->GetData());
    idx_++;
    if (idx_ >= leaf_page->GetSize() &&
        INVALID_PAGE_ID != leaf_page->GetNextPageId()){
        cur_page_id_ = leaf_page->GetNextPageId();
        idx_ = 0;
    }
    return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
