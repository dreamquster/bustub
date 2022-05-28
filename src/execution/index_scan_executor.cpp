//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), indexIter(INVALID_PAGE_ID, 0, exec_ctx->GetBufferPoolManager()) {
  this->plan_ = plan;
  auto indexOid = plan_->GetIndexOid();
  indexInfo_ = exec_ctx_->GetCatalog()->GetIndex(indexOid);

}

void IndexScanExecutor::Init() {
  bPlusTreeIndex =  (BPlusTreeIndex<GenericKey<64>, RID, GenericComparator<64>>*)(indexInfo_->index_.get());
  indexIter = bPlusTreeIndex->GetBeginIterator();
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  if (indexIter == bPlusTreeIndex->GetEndIterator()) {
    return false;
  }
  ++indexIter;
  auto key = *indexIter;
  *rid = key.second;

  auto schema = plan_->OutputSchema();
  std::vector<Value> vals;
  vals.reserve(schema->GetColumnCount());
  // todo: fixed how to get key value
  return false;
}

}  // namespace bustub
