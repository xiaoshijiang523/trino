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

package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;

import java.util.Optional;

import static io.trino.SystemSessionProperties.isCTEReuseEnabled;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.partitionedExchange;
import static io.trino.sql.planner.plan.Patterns.cteScan;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.source;

public class AddExchangeAboveCTENode
        implements Rule<FilterNode>
{
    private static final Pattern<FilterNode> PATTERN = filter()
            .with(source().matching(cteScan()));

    @Override
    public boolean isEnabled(Session session)
    {
        return isCTEReuseEnabled(session);
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(FilterNode node, Captures captures, Context context)
    {
        PlanNode source = node.getSource();
        source = partitionedExchange(
                context.getIdAllocator().getNextId(),
                REMOTE,
                source,
                source.getOutputSymbols(),
                Optional.empty());
        PlanNode newFilterNode = node.replaceChildren(ImmutableList.of(source));
        return Result.ofPlanNode(newFilterNode);
    }
}
