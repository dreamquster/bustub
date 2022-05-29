//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  left_executor_ = std::move(left_executor);
  right_executor_ = std::move(right_executor);
}

void NestedLoopJoinExecutor::Init() {
  cur_idx_ = 0;

  Tuple left_tuple;
  RID left_rid;
  Tuple right_tuple;
  RID right_rid;
  left_executor_->Init();
  auto left_plan = plan_->GetLeftPlan();
  auto right_plan = plan_->GetRightPlan();
  auto predicate = plan_->Predicate();
  auto out_schema = plan_->OutputSchema();
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    right_executor_->Init();
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      if (nullptr == predicate
          ||  predicate->EvaluateJoin(&left_tuple, left_plan->OutputSchema(),
                                     &right_tuple, right_plan->OutputSchema()).GetAs<bool>()) {
        std::vector<Value> output;
        for (const auto & column : out_schema->GetColumns()) {
          auto val = column.GetExpr()->EvaluateJoin(&left_tuple, left_plan->OutputSchema(),
                                         &right_tuple, right_plan->OutputSchema());
          output.push_back(val);
        }
        join_result_.emplace_back(Tuple(output, out_schema));
      }
    }
  }
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (cur_idx_ >= join_result_.size()) {
    return false;
  }
  *tuple = join_result_[cur_idx_];
  cur_idx_++;
  return true;
}

}  // namespace bustub
