//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx)  {
  this->plan_ = plan;
  childExecutor_ = std::move(child_executor);
}

void InsertExecutor::Init() {
  catalog_ = exec_ctx_->GetCatalog();
  tableMetadata_ = catalog_->GetTable(plan_->TableOid());
  tableHeap_ =  tableMetadata_->table_.get();
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (plan_->IsRawInsert()) {
    for (auto& rowVal: plan_->RawValues()) {
      InsertIntoWithIndex(Tuple(rowVal, &(tableMetadata_->schema_)));
    }
    return false;
  }
  // execute child
  std::vector<Tuple> resultSet;
  childExecutor_->Init();
  try {
    Tuple childTuple;
    RID childRid;
    while (childExecutor_->Next(&childTuple, &childRid)) {
      resultSet.push_back(childTuple);
    }
  } catch (Exception &e) {
    // TODO(student): handle exceptions
    throw Exception("failed child execution in InsertExecutor");
  }
  for (auto rowValues : resultSet) {
    InsertIntoWithIndex(rowValues);
  }
  return false;
}
void InsertExecutor::InsertIntoWithIndex(Tuple tuple) {
  RID rid;
  if (!tableHeap_->InsertTuple(tuple, &rid, exec_ctx_->GetTransaction())) {
    throw Exception("no memery for insert");
  }
  for (auto& index : catalog_->GetTableIndexes(tableMetadata_->name_)) {
    index->index_->InsertEntry(tuple.KeyFromTuple(tableMetadata_->schema_,
                                                        index->key_schema_,
                                                        index->index_->GetKeyAttrs()),
                                     rid, exec_ctx_->GetTransaction());
  }
}

}  // namespace bustub
