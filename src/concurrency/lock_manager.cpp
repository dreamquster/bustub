//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

#include <utility>
#include <vector>

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> ul(latch_);
shareGet:
    LockRequestQueue& lock_queue = lock_table_[rid];
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }

    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      txn->SetState(TransactionState::ABORTED);
      return false;
    }

    // can't get new lock in shrinking stage
    if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
      return false;
    }

    if (txn->IsSharedLocked(rid)) {
      return true;
    }
    // get shared lock
    auto lock_txn_it = lock_queue.request_queue_.begin();
    // release larger transId exclusive lock
    while (lock_txn_it != lock_queue.request_queue_.end()) {
        Transaction* lock_txn = TransactionManager::GetTransaction(lock_txn_it->txn_id_);
        if (lock_txn->GetTransactionId() > txn->GetTransactionId()
                && lock_txn->GetExclusiveLockSet()->count(rid) != 0) {
          // current transaction is older than lock_txn
          lock_queue.request_queue_.erase(lock_txn_it);
          abortTxnAndUnlock(rid, lock_txn);
        } else if (lock_txn->GetTransactionId() < txn->GetTransactionId()
                   && lock_txn->GetExclusiveLockSet()->count(rid) != 0) {
          // wait shared lock
          insertTxnLockQueue(lock_queue, txn->GetTransactionId(), LockMode::SHARED);
          txn->GetSharedLockSet()->emplace(rid);
          lock_queue.cv_.wait(ul);
          goto shareGet;
        } else {
          lock_txn_it++;
        }
    }
    txn->SetState(TransactionState::GROWING);

    insertTxnLockQueue(lock_queue, txn->GetTransactionId(), LockMode::SHARED);
    txn->GetSharedLockSet()->emplace(rid);


  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock ul(latch_);
  auto& lock_queue = lock_table_[rid];
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (txn->GetState() == TransactionState::SHRINKING
        && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  auto lock_txn_it = lock_queue.request_queue_.begin();
  while (lock_txn_it != lock_queue.request_queue_.end()) {
    Transaction* lock_txn = TransactionManager::GetTransaction(lock_txn_it->txn_id_);
    if (lock_txn->GetTransactionId() > txn->GetTransactionId()) {
      lock_queue.request_queue_.erase(lock_txn_it);
      abortTxnAndUnlock(rid, lock_txn);
    } else if (lock_txn->GetTransactionId() < txn->GetTransactionId()) {
      abortTxnAndUnlock(rid, txn);
      return false;
    } else {
      lock_txn_it++;
    }
  }

  // acquired exclusive lock
  txn->SetState(TransactionState::GROWING);
  insertTxnLockQueue(lock_queue, txn->GetTransactionId(), LockMode::EXCLUSIVE);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}
void LockManager::abortTxnAndUnlock(const RID &rid, Transaction *lock_txn) const {
  lock_txn->GetExclusiveLockSet()->erase(rid);
  lock_txn->GetSharedLockSet()->erase(rid);
  lock_txn->SetState(TransactionState::ABORTED);
}

void LockManager::insertTxnLockQueue(LockRequestQueue& lock_queue, txn_id_t txn_id, LockMode lock_mode) {
  bool is_inserted = false;
  for (auto& it : lock_queue.request_queue_) {
    if (it.txn_id_ == txn_id) {
      is_inserted = true;
      it.granted_ = (lock_mode == LockMode::EXCLUSIVE);
      break;
    }
  }
  if (!is_inserted) {
    lock_queue.request_queue_.emplace_back(txn_id, lock_mode);
  }
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock ul(latch_);
upgradeCheck:

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (txn->GetState() == TransactionState::SHRINKING
        && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  auto& lock_queue = lock_table_[rid];
  if (lock_queue.upgrading_) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  lock_queue.upgrading_ = true;

  auto lock_txn_it = lock_queue.request_queue_.begin();
  while (lock_txn_it != lock_queue.request_queue_.end()) {
    Transaction *lock_txn = TransactionManager::GetTransaction(lock_txn_it->txn_id_);
    if (lock_txn->GetTransactionId() > txn->GetTransactionId()) {
      lock_queue.request_queue_.erase(lock_txn_it);
      abortTxnAndUnlock(rid, lock_txn);
    } else if (lock_txn->GetTransactionId() < txn->GetTransactionId()) {
      lock_queue.cv_.wait(ul);
      goto upgradeCheck;
    } else {
      lock_txn_it++;
    }
  }

  //
  txn->SetState(TransactionState::GROWING);

  auto& request_item = lock_queue.request_queue_.front();
  request_item.lock_mode_ = LockMode::EXCLUSIVE;
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  lock_queue.upgrading_ = false;
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock ul(latch_);

  auto& lock_queue = lock_table_[rid];


  if (txn->GetState() == TransactionState::GROWING
        && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::SHRINKING);
  }

  auto txn_lockmode = txn->IsSharedLocked(rid) ? LockMode::SHARED
                                               : LockMode::EXCLUSIVE;
  auto& requst_queue = lock_queue.request_queue_;
  auto lock_txn_it = std::find_if(requst_queue.begin(), requst_queue.end(), [&](const LockRequest& lock_req) {
    return lock_req.txn_id_ == txn->GetTransactionId();
  });

  if (lock_txn_it != requst_queue.end()) {
    requst_queue.erase(lock_txn_it);
    switch (txn_lockmode) {
      case LockMode::SHARED:
        txn->GetSharedLockSet()->erase(rid);
        if (!requst_queue.empty()) {
          lock_queue.cv_.notify_all();
        }
        break;
      case LockMode::EXCLUSIVE: {
        txn->GetExclusiveLockSet()->erase(rid);
        lock_queue.cv_.notify_all();
        break;
      }
    }
    return true;
  }


  return false;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

bool LockManager::HasCycle(txn_id_t *txn_id) { return false; }

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() { return {}; }

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // TODO(student): remove the continue and add your cycle detection and abort code here
      continue;
    }
  }
}

}  // namespace bustub
