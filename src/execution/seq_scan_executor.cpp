//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx),
  tableIter_(nullptr, RID(), nullptr){
  this->plan_ = plan;
  auto tableOid = plan_->GetTableOid();
  tableMetadata = exec_ctx_->GetCatalog()->GetTable(tableOid);
  table_ = std::move(tableMetadata->table_);
}

void SeqScanExecutor::Init() {
  tableIter_ = table_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  if (tableIter_ == table_->End()) {
    return false;
  }
  auto curRid = tableIter_->GetRid();
  auto schema = plan_->OutputSchema();
  std::vector<Value> vals;
  vals.reserve(schema->GetColumnCount());
  for (size_t i = 0; i < vals.capacity(); i++) {
    vals.push_back(schema->GetColumn(i).GetExpr()->Evaluate(
        &(*tableIter_), &(tableMetadata->schema_)));
  }

  ++tableIter_;

  Tuple rowTuple(vals, schema);
  auto predicate = plan_->GetPredicate();
  if (nullptr == predicate ||
        (predicate->Evaluate(&rowTuple, schema).GetAs<bool>())) {
    *tuple = rowTuple;
    *rid = curRid;
    return true;
  }


  return Next(tuple, rid);
}

}  // namespace bustub
