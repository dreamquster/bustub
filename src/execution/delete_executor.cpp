//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_=std::move(child_executor);
}

void DeleteExecutor::Init() {
  catalog_ = exec_ctx_->GetCatalog();
  table_info_ = catalog_->GetTable(plan_->TableOid());
  table_heap_ = table_info_->table_.get();
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple delTuple;
  RID delRid;
  while (child_executor_->Next(&delTuple, &delRid)) {
    if (!table_heap_->MarkDelete(delRid, exec_ctx_->GetTransaction())) {
      throw Exception("mark delete rid:" + delRid.ToString() + " failed");
    }

    for (auto& index : catalog_->GetTableIndexes(table_info_->name_)) {
      index->index_->DeleteEntry(
          delTuple.KeyFromTuple(table_info_->schema_,
                                                    index->key_schema_,
                                                    index->index_->GetKeyAttrs()),
                                 delRid, exec_ctx_->GetTransaction());
    }
  }
  return false;
}

}  // namespace bustub
