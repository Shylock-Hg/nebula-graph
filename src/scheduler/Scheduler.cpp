/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "scheduler/Scheduler.h"

#include "context/QueryContext.h"
#include "executor/ExecutionError.h"
#include "executor/Executor.h"
#include "executor/logic/LoopExecutor.h"
#include "executor/logic/PassThroughExecutor.h"
#include "executor/logic/SelectExecutor.h"
#include "planner/plan/Logic.h"
#include "planner/plan/PlanNode.h"
#include "planner/plan/Query.h"

namespace nebula {
namespace graph {

/*static*/ void Scheduler::analyzeLifetime(PlanNode* root, bool inLoop) {
    std::stack<std::tuple<PlanNode*, bool>> stack;
    stack.push(std::make_tuple(root, inLoop));
    while (!stack.empty()) {
        const auto& current = stack.top();
        PlanNode* currentNode = std::get<0>(current);
        const auto currentInLoop = std::get<1>(current);
        for (auto& inputVar : currentNode->inputVars()) {
            if (inputVar != nullptr) {
                inputVar->setLastUser(currentNode->id());
            }
        }
        if (currentNode->kind() == PlanNode::Kind::kLoop || currentInLoop) {
            currentNode->setInLoop(true);
        }
        stack.pop();

        for (auto dep : currentNode->dependencies()) {
            stack.push(std::make_tuple(const_cast<PlanNode*>(dep), currentInLoop));
        }
        switch (currentNode->kind()) {
            case PlanNode::Kind::kSelect: {
                auto sel = static_cast<Select*>(currentNode);
                stack.push(std::make_tuple(sel->then(), currentInLoop));
                stack.push(std::make_tuple(sel->otherwise(), currentInLoop));
                // the scheduler will use it
                sel->outputVarPtr()->setLastUser(-1);
                break;
            }
            case PlanNode::Kind::kLoop: {
                auto loop = static_cast<Loop*>(currentNode);
                stack.push(std::make_tuple(loop->body(), true));
                // the scheduler will use it
                loop->outputVarPtr()->setLastUser(-1);
                break;
            }
            default:
                break;
        }
    }
}

}   // namespace graph
}   // namespace nebula
