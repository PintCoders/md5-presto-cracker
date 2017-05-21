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
package com.facebook.presto.cracker;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class CrackerSplitManager implements ConnectorSplitManager
{
    private final NodeManager nodeManager;
    private final int splitsPerNode;

    public CrackerSplitManager(NodeManager nodeManager, int splitsPerNode)
    {
        this.nodeManager = nodeManager;
        checkArgument(splitsPerNode > 0, "splitsPerNode must be at least 1");
        this.splitsPerNode = splitsPerNode;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        CrackerTableHandle crackerTableHandle = Types.checkType(layout, CrackerTableLayoutHandle.class, "layout").getTable();

        // Worker Node
        List<Node> nodes = new ArrayList<>();
        nodes.addAll(nodeManager.getRequiredWorkerNodes());

        int numberOfSplit = nodes.size() * splitsPerNode;
        long subRangeSize = (CrackerUtil.TOTAL_PASSWORD_RANGE_SIZE + numberOfSplit - 1) / numberOfSplit;

        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

        for (int i = 0; i < splitsPerNode * nodes.size(); i++) {
            long subRangeBegin = i * subRangeSize;
            long subRangeEnd = (i + 1) * subRangeSize;

            if (subRangeEnd >= CrackerUtil.TOTAL_PASSWORD_RANGE_SIZE) {
                subRangeEnd = CrackerUtil.TOTAL_PASSWORD_RANGE_SIZE;
            }
            int nodeNumber = i / splitsPerNode;
            splits.add(new CrackerSplit(crackerTableHandle, subRangeBegin, subRangeEnd, ImmutableList.of(nodes.get(nodeNumber).getHostAndPort())));
        }
        return new FixedSplitSource(splits.build());
    }
}
