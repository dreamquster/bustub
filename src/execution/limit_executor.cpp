//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void LimitExecutor::Init() {
  cur_idx_ = 0;
  child_executor_->Init();
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  if (cur_idx_ >= plan_->GetLimit()) {
    return false;
  }
  cur_idx_++;
  Tuple next_tuple;
  RID next_rid;
  if (!child_executor_->Next(&next_tuple, &next_rid)) {
    return false;
  }

  *tuple = next_tuple;
  *rid = next_rid;

  return true;
}

}  // namespace bustub
