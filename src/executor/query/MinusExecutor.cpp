/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/MinusExecutor.h"

#include <unordered_set>

#include "planner/Query.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> MinusExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    NG_RETURN_IF_ERROR(checkInputDataSets());

    auto left = getLeftInputData();
    auto right = getRightInputData();

    std::unordered_set<const LogicalRow *> hashSet;
    for (; right.iterRef()->valid(); right.iterRef()->next()) {
        hashSet.insert(right.iterRef()->row());
        // TODO: should test duplicate rows
    }

    if (!hashSet.empty()) {
        while (left.iterRef()->valid()) {
            auto iter = hashSet.find(left.iterRef()->row());
            if (iter == hashSet.end()) {
                left.iterRef()->next();
            } else {
                left.iterRef()->unstableErase();
            }
        }
    }

    ResultBuilder builder;
    builder.values(left.values()).iter(std::move(left).iter());
    return finish(builder.finish());
}

}   // namespace graph
}   // namespace nebula
