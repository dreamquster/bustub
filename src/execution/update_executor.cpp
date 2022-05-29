//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_=std::move(child_executor);
}

void UpdateExecutor::Init() {
  catalog_ = exec_ctx_->GetCatalog();
  table_info_ = catalog_->GetTable(plan_->TableOid());
  table_heap_ = table_info_->table_.get();
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {

  Tuple oldTuple;
  RID oldRid;
  while (child_executor_->Next(&oldTuple, &oldRid)) {
      auto updatedTuple = GenerateUpdatedTuple(oldTuple);
      if (!table_heap_->UpdateTuple(updatedTuple, oldRid, exec_ctx_->GetTransaction())) {
        throw Exception("update rid:"+ oldRid.ToString() +" failed in UpdateExecutor");
      }
  }
  return false;
}
}  // namespace bustub
