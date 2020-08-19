/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_GOVALIDATOR_H_
#define VALIDATOR_GOVALIDATOR_H_

#include "common/base/Base.h"
#include "validator/TraversalValidator.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {
class GoValidator final : public TraversalValidator {
public:
    GoValidator(Sentence* sentence, QueryContext* context)
        : TraversalValidator(sentence, context) {}

private:
    Status validateImpl() override;

    Status toPlan() override;

    Status validateOver(const OverClause* over);

    Status validateWhere(WhereClause* where);

    Status validateYield(YieldClause* yield);

    void extractPropExprs(const Expression* expr);

    std::unique_ptr<Expression> rewriteToInputProp(Expression* expr);

    Status buildColumns();

    Status buildOneStepPlan();

    Status buildNStepsPlan();

    Status buildMToNPlan();

    Status oneStep(PlanNode* dependencyForGn, const std::string& inputVarNameForGN,
                   PlanNode* projectFromJoin);

    GetNeighbors::VertexProps buildSrcVertexProps();

    std::vector<storage::cpp2::VertexProp> buildDstVertexProps();

    GetNeighbors::EdgeProps buildEdgeProps();

    GetNeighbors::EdgeProps buildEdgeDst();

    void buildEdgeProps(GetNeighbors::EdgeProps& edgeProps, bool isInEdge);

    Project* buildLeftVarForTraceJoin(PlanNode* projectStartVid);

    Project* traceToStartVid(Project* projectLeftVarForJoin,
                             Project* projectDstFromGN);

    PlanNode* buildJoinPipeOrVariableInput(PlanNode* projectFromJoin,
                                           PlanNode* dependencyForJoinInput);

    PlanNode* buildProjectSrcEdgePropsForGN(std::string gnVar, PlanNode* dependency);

    PlanNode* buildJoinDstProps(PlanNode* projectSrcDstProps);

private:
    bool                                                    isOverAll_{false};
    std::vector<EdgeType>                                   edgeTypes_;
    storage::cpp2::EdgeDirection                            direction_;
    Expression*                                             filter_{nullptr};
    std::vector<std::string>                                colNames_;
    YieldColumns*                                           yields_{nullptr};
    bool                                                    distinct_{false};

    // Generated by validator if needed, and the lifecycle of raw pinters would
    // be managed by object pool
    YieldColumns*                                           srcAndEdgePropCols_{nullptr};
    YieldColumns*                                           dstPropCols_{nullptr};
    YieldColumns*                                           inputPropCols_{nullptr};
    std::unordered_map<std::string, YieldColumn*>           propExprColMap_;
    Expression*                                             newFilter_{nullptr};
    YieldColumns*                                           newYieldCols_{nullptr};
    // Used for n steps to trace the path
    std::string                                             dstVidColName_;
    // Used for get dst props
    std::string                                             joinDstVidColName_;
    std::vector<std::string>                                allEdges_;
};
}  // namespace graph
}  // namespace nebula
#endif
