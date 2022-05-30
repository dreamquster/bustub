//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  child_executor_ = std::move(child_executor);
}

void NestIndexJoinExecutor::Init() {
  cur_idx_ = 0;
  Tuple out_tuple;
  RID out_rid;
  auto output_schema = plan_->OutputSchema();
  child_executor_->Init();
  auto predicate = plan_->Predicate();
  auto inner_schema = plan_->InnerTableSchema();
  auto outer_schema = plan_->OuterTableSchema();
  auto inner_table = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(inner_table->name_, plan_->GetIndexName());
  auto bplus_tree_index =  (BPlusTreeIndex<GenericKey<64>, RID, GenericComparator<64>>*)(index_info->index_.get());
  while (child_executor_->Next(&out_tuple, &out_rid)) {
    for (auto iter = bplus_tree_index->GetBeginIterator();
            iter != bplus_tree_index->GetEndIterator(); ++iter) {
      auto key = *iter;
      Tuple inner_tuple;
      if (inner_table->table_->GetTuple(key.second, &inner_tuple, exec_ctx_->GetTransaction())) {
        if (nullptr == predicate
              || predicate->EvaluateJoin(&out_tuple, outer_schema, &inner_tuple, inner_schema).GetAs<bool>()) {
          std::vector<Value> output;
          for (const auto & column : output_schema->GetColumns()) {
            auto val = column.GetExpr()->EvaluateJoin(&out_tuple, outer_schema,
                                                      &inner_tuple, inner_schema);
            output.push_back(val);
          }
          join_result_.emplace_back(Tuple(output, output_schema));
        }
      }
    }

  }
}

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (cur_idx_ >= join_result_.size()) {
    return false;
  }
  *tuple = join_result_[cur_idx_];
  cur_idx_++;
  return true;
}

}  // namespace bustub
