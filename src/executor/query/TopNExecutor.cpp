/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/TopNExecutor.h"
#include "planner/Query.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> TopNExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto* topn = asNode<TopN>(node());
    Result result = ectx_->getResult(topn->inputVar());
    if (UNLIKELY(result.iterRef() == nullptr)) {
        return Status::Error("Internal error: nullptr iterator in topn executor");
    }
    if (UNLIKELY(result.iterRef()->isDefaultIter())) {
        std::string errMsg = "Internal error: Sort executor does not supported DefaultIter";
        LOG(ERROR) << errMsg;
        return Status::Error(errMsg);
    }
    if (UNLIKELY(result.iterRef()->isGetNeighborsIter())) {
        std::string errMsg = "Internal error: TopN executor does not supported GetNeighborsIter";
        LOG(ERROR) << errMsg;
        return Status::Error(errMsg);
    }

    auto &factors = topn->factors();
    comparator_ = [&factors] (const LogicalRow &lhs, const LogicalRow &rhs) {
        for (auto &item : factors) {
            auto index = item.first;
            auto orderType = item.second;
            if (lhs[index] == rhs[index]) {
                continue;
            }

            if (orderType == OrderFactor::OrderType::ASCEND) {
                return lhs[index] < rhs[index];
            } else if (orderType == OrderFactor::OrderType::DESCEND) {
                return lhs[index] > rhs[index];
            }
        }
        return false;
    };

    offset_ = topn->offset();
    auto count = topn->count();
    auto size = result.iterRef()->size();
    maxCount_ = count;
    heapSize_ = 0;
    if (size <= static_cast<size_t>(offset_)) {
        maxCount_ = 0;
    } else if (size > static_cast<size_t>(offset_ + count)) {
        heapSize_ = offset_ + count;
    } else {
        maxCount_ = size - offset_;
        heapSize_ = size;
    }
    if (heapSize_ == 0) {
        result.iterRef()->clear();
        return finish(ResultBuilder()
            .values(result.values()).iter(std::move(result).iter()).finish());
    }

    if (result.iterRef()->isSequentialIter()) {
        executeTopN<SequentialIter::SeqLogicalRow, SequentialIter>(result.iterRef());
    } else if (result.iterRef()->isJoinIter()) {
        executeTopN<JoinIter::JoinLogicalRow, JoinIter>(result.iterRef());
    } else if (result.iterRef()->isPropIter()) {
        executeTopN<PropIter::PropLogicalRow, PropIter>(result.iterRef());
    }
    result.iterRef()->eraseRange(maxCount_, size);
    return finish(ResultBuilder().values(result.values()).iter(std::move(result).iter()).finish());
}

template<typename T, typename U>
void TopNExecutor::executeTopN(Iterator *iter) {
    auto uIter = static_cast<U*>(iter);
    std::vector<T> heap(uIter->begin(), uIter->begin()+heapSize_);
    std::make_heap(heap.begin(), heap.end(), comparator_);
    auto it = uIter->begin() + heapSize_;
    while (it != uIter->end()) {
        if (comparator_(*it, heap[0])) {
            std::pop_heap(heap.begin(), heap.end(), comparator_);
            heap.pop_back();
            heap.push_back(*it);
            std::push_heap(heap.begin(), heap.end(), comparator_);
        }
        ++it;
    }
    std::sort_heap(heap.begin(), heap.end(), comparator_);

    auto beg = uIter->begin();
    for (int i = 0; i < maxCount_; ++i) {
        beg[i] = heap[offset_+i];
    }
}

}   // namespace graph
}   // namespace nebula
