/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.trino.sql.planner.optimizations;

import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.CTEScanNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.Expression;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.trino.SystemSessionProperties.isCTEReuseEnabled;
import static java.util.Objects.requireNonNull;

/*
 * If any specific CTE is being used only once, then remove the CTE node from the plan.
 */
public class PruneCTENodes
        implements PlanOptimizer
{
    private final Metadata metadata;
    private final TypeAnalyzer typeAnalyzer;
    private final boolean pruneCTEWithCrossJoin;

    public PruneCTENodes(Metadata metadata, TypeAnalyzer typeAnalyzer, boolean pruneCTEWithCrossJoin)
    {
        this.metadata = metadata;
        this.typeAnalyzer = typeAnalyzer;
        this.pruneCTEWithCrossJoin = pruneCTEWithCrossJoin;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator,
                             PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        if (!isCTEReuseEnabled(session)) {
            return plan;
        }
        else {
            OptimizedPlanRewriter optimizedPlanRewriter = new OptimizedPlanRewriter(metadata, typeAnalyzer, false, pruneCTEWithCrossJoin);
            PlanNode newNode = SimplePlanRewriter.rewriteWith(optimizedPlanRewriter, plan);
            if (optimizedPlanRewriter.isSecondTraverseRequired()) {
                return SimplePlanRewriter.rewriteWith(optimizedPlanRewriter, newNode);
            }

            return newNode;
        }
    }

    private static class OptimizedPlanRewriter
            extends SimplePlanRewriter<Expression>
    {
        private final Metadata metadata;
        private final TypeAnalyzer typeAnalyzer;
        private boolean isNodeAlreadyVisited;
        private final boolean pruneCTEWithCrossJoin;
        private Set<Integer> cTEWithCrossJoinList = new HashSet<>();

        private final Map<Integer, Integer> cteUsageMap;
        private final Set<Integer> cteToPrune; //because of dynamic filter not matching

        private OptimizedPlanRewriter(Metadata metadata, TypeAnalyzer typeAnalyzer, Boolean isNodeAlreadyVisited, boolean pruneCTEWithCrossJoin)
        {
            this.metadata = metadata;
            this.typeAnalyzer = typeAnalyzer;
            this.isNodeAlreadyVisited = isNodeAlreadyVisited;
            this.cteUsageMap = new HashMap<>();
            this.pruneCTEWithCrossJoin = pruneCTEWithCrossJoin;
            cteToPrune = new HashSet<>();
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Expression> context)
        {
            if (pruneCTEWithCrossJoin && node.isCrossJoin()) {
                Integer left = getChildCTERefNum(node.getLeft());
                Integer right = getChildCTERefNum(node.getRight());
                if (left != null && right != null && left.equals(right)) {
                    cTEWithCrossJoinList.add(left);
                }
            }
            return context.defaultRewrite(node, context.get());
        }

        private Integer getChildCTERefNum(PlanNode node)
        {
            if (node instanceof CTEScanNode) {
                return ((CTEScanNode) node).getCommonCTERefNum();
            }
            else if (node instanceof ProjectNode) {
                return getChildCTERefNum(((ProjectNode) node).getSource());
            }
            else if (node instanceof FilterNode) {
                return getChildCTERefNum(((FilterNode) node).getSource());
            }
            else if (node.getSources().size() == 1 && node instanceof ExchangeNode) {
                return getChildCTERefNum(node.getSources().get(0));
            }
            return null;
        }

        @Override
        public PlanNode visitCTEScan(CTEScanNode inputNode, RewriteContext<Expression> context)
        {
            CTEScanNode node = inputNode;
            Integer commonCTERefNum = node.getCommonCTERefNum();
            if (pruneCTEWithCrossJoin) {
                if (cTEWithCrossJoinList.contains(commonCTERefNum)) {
                    node = (CTEScanNode) visitPlan(node, context);
                    return node.getSource();
                }

                // If there is a self join below CTE node, then CTE should be removed.
                if (node.getSource() instanceof JoinNode) {
                    // check if this join is self join
                    TableHandle left = getTableHandle(((JoinNode) node.getSource()).getLeft());
                    TableHandle right = getTableHandle(((JoinNode) node.getSource()).getRight());
                    if (left != null && right != null && left.getConnectorHandle().equals(right.getConnectorHandle())) {
                        // both tables are same, means it is self join.
                        node = (CTEScanNode) visitPlan(node, context);
                        return node.getSource();
                    }
                }
            }
            if (!isNodeAlreadyVisited) {
                cteUsageMap.merge(commonCTERefNum, 1, Integer::sum);
            }
            else {
                if (cteUsageMap.get(commonCTERefNum) == 1 || cteToPrune.contains(commonCTERefNum)) {
                    node = (CTEScanNode) visitPlan(node, context);
                    return node.getSource();
                }
            }
            return visitPlan(node, context);
        }

        private TableHandle getTableHandle(PlanNode node)
        {
            if (node instanceof TableScanNode) {
                return ((TableScanNode) node).getTable();
            }
            else if (node instanceof ProjectNode) {
                return getTableHandle(((ProjectNode) node).getSource());
            }
            else if (node instanceof FilterNode) {
                return getTableHandle(((FilterNode) node).getSource());
            }
            else if (node.getSources().size() == 1 && node instanceof ExchangeNode) {
                return getTableHandle(node.getSources().get(0));
            }
            return null;
        }

        // If only there was any CTE with just one usage, we need to traverse again to remove CTE node otherwise no need.
        private boolean isSecondTraverseRequired()
        {
            isNodeAlreadyVisited = cteUsageMap.size() != 0 && cteUsageMap.values().stream().filter(x -> x <= 1).count() > 0
                    || cteToPrune.size() > 0;
            return isNodeAlreadyVisited;
        }
    }
}
