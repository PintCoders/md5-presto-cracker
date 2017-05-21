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

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;

import java.util.Map;

import static com.facebook.presto.cracker.CrackerConsts.SPLITS_PER_NODE;

public class CrackerConnectorFactory implements ConnectorFactory
{
    private final int defaultSplitsPerNode;

    public CrackerConnectorFactory()
    {
        this(SPLITS_PER_NODE);
    }

    public CrackerConnectorFactory(int defaultSplitsPerNode)
    {
        this.defaultSplitsPerNode = defaultSplitsPerNode;
    }

    @Override
    public String getName()
    {
        return "cracker";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new CrackerHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config, ConnectorContext context)
    {
        NodeManager nodeManager = context.getNodeManager();
        return new Connector()
        {
            @Override
            public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
            {
                return CrackerTransactionHandle.INSTANSE;
            }

            @Override
            public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
            {
                return new CrackerMetadata(connectorId);
            }

            @Override
            public ConnectorSplitManager getSplitManager()
            {
                return new CrackerSplitManager(nodeManager, defaultSplitsPerNode);
            }

            @Override
            public ConnectorRecordSetProvider getRecordSetProvider()
            {
                return new CrackerRecordSetProvider();
            }
        };
    }
}
