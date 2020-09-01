/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_VALIDATOR_H_
#define VALIDATOR_VALIDATOR_H_

#include "common/base/Base.h"
#include "service/PermissionCheck.h"
#include "planner/ExecutionPlan.h"
#include "parser/Sentence.h"
#include "context/ValidateContext.h"
#include "context/QueryContext.h"

namespace nebula {

class YieldColumns;

namespace graph {

class ExpressionProps final {
public:
    using TagIDPropsMap =  std::unordered_map<TagID, std::set<folly::StringPiece>>;
    using EdgePropMap = std::unordered_map<EdgeType, std::set<folly::StringPiece>>;
    using VarPropMap = std::unordered_map<std::string, std::set<folly::StringPiece>>;

    void insertInputProp(folly::StringPiece prop);

    void insertVarProp(const std::string& varName, folly::StringPiece prop);

    void insertSrcTagProp(TagID tagId, folly::StringPiece prop);

    void insertDstTagProp(TagID tagId, folly::StringPiece prop);

    void insertEdgeProp(EdgeType edgeType, folly::StringPiece prop);

    void insertTagProp(TagID tagId, folly::StringPiece prop);

    std::set<folly::StringPiece>& inputProps() {
        return inputProps_;
    }
    TagIDPropsMap& srcTagProps() {
        return srcTagProps_;
    }
    TagIDPropsMap& dstTagProps() {
        return dstTagProps_;
    }
    TagIDPropsMap& tagProps() {
        return tagProps_;
    }
    EdgePropMap& edgeProps() {
        return edgeProps_;
    }
    VarPropMap& varProps() {
        return varProps_;
    }

    bool isSubsetOfInput(const std::set<folly::StringPiece>& props);

    bool isSubsetOfVar(const VarPropMap& props);

    void unionProps(ExpressionProps exprProps);

private:
    std::set<folly::StringPiece>  inputProps_;
    VarPropMap                    varProps_;
    TagIDPropsMap                 srcTagProps_;
    TagIDPropsMap                 dstTagProps_;
    EdgePropMap                   edgeProps_;
    TagIDPropsMap                 tagProps_;
};

class Validator {
public:
    virtual ~Validator() = default;

    static std::unique_ptr<Validator> makeValidator(Sentence* sentence,
                                                    QueryContext* context);

    static Status validate(Sentence* sentence, QueryContext* qctx);

    Status validate();

    MUST_USE_RESULT Status appendPlan(PlanNode* tail);

    void setInputVarName(std::string name) {
        inputVarName_ = std::move(name);
    }

    void setInputCols(ColsDef&& inputs) {
        inputs_ = std::move(inputs);
    }

    PlanNode* root() const {
        return root_;
    }

    PlanNode* tail() const {
        return tail_;
    }

    ColsDef outputCols() const {
        return outputs_;
    }

    ColsDef inputCols() const {
        return inputs_;
    }

    void setNoSpaceRequired() {
        noSpaceRequired_ = true;
    }

protected:
    Validator(Sentence* sentence, QueryContext* qctx);

    // So the validate call `spaceChosen` -> `validateImpl` -> `checkPermission` -> `toPlan`
    // in order

    /**
     * Check if a space is chosen for this sentence.
     */
    virtual bool spaceChosen();

    // Do all permission checking in validator except which need execute
    // TODO(shylock) do all permission which don't need execute in here
    virtual Status checkPermission() {
        return PermissionCheck::permissionCheck(qctx_->rctx()->session(), sentence_, space_.id);
    }

    /**
     * Validate the sentence.
     */
    virtual Status validateImpl() = 0;

    /**
     * Convert an ast to plan.
     */
    virtual Status toPlan() = 0;

    std::vector<std::string> deduceColNames(const YieldColumns* cols) const;

    std::string deduceColName(const YieldColumn* col) const;

    StatusOr<Value::Type> deduceExprType(const Expression* expr) const;

    Status deduceProps(const Expression* expr, ExpressionProps& exprProps);

    bool evaluableExpr(const Expression* expr) const;

    static Status checkPropNonexistOrDuplicate(const ColsDef& cols,
                                               const folly::StringPiece& prop,
                                               const std::string &validatorName);

    static Status appendPlan(PlanNode* plan, PlanNode* appended);

    // use for simple Plan only contain one node
    template <typename Node, typename... Args>
    Status genSingleNodePlan(Args... args) {
        auto *doNode = Node::make(qctx_, nullptr, std::forward<Args>(args)...);
        root_ = doNode;
        tail_ = root_;
        return Status::OK();
    }

    // Check the variable or input property reference
    // return the input variable
    StatusOr<std::string> checkRef(const Expression *ref, const Value::Type type) const;

protected:
    SpaceDescription                space_;
    Sentence*                       sentence_{nullptr};
    QueryContext*                   qctx_{nullptr};
    ValidateContext*                vctx_{nullptr};
    // root and tail of a subplan.
    PlanNode*                       root_{nullptr};
    PlanNode*                       tail_{nullptr};
    // The input columns and output columns of a sentence.
    ColsDef                         outputs_;
    ColsDef                         inputs_;
    // The variable name of the input node.
    std::string                     inputVarName_;
    // Admin sentences do not requires a space to be chosen.
    bool                            noSpaceRequired_{false};
};

}  // namespace graph
}  // namespace nebula
#endif
