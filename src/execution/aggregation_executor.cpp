//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {
  plan_ = plan;
  child_ = std::move(child);
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  aht_iterator_ = aht_.Begin();
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    aht_.InsertCombine(MakeKey(&tuple), MakeVal(&tuple));
  }
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  const auto agg_key = aht_iterator_.Key();
  const auto agg_val = aht_iterator_.Val();
  ++aht_iterator_;
  auto having_ = plan_->GetHaving();
  if (nullptr == having_
        || having_->EvaluateAggregate(agg_key.group_bys_, agg_val.aggregates_).GetAs<bool>()) {
    std::vector<Value> ret;
    auto schema = plan_->OutputSchema();
    for (const auto & column : schema->GetColumns()) {
      auto val = column.GetExpr()->EvaluateAggregate(agg_key.group_bys_, agg_val.aggregates_);
      ret.push_back(val);
    }
    *tuple = Tuple(ret, schema);
    return true;
  }

  return Next(tuple, rid);
}

}  // namespace bustub
