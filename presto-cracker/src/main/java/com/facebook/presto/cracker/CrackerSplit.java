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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CrackerSplit implements ConnectorSplit
{
    private final CrackerTableHandle tableHandle;
    private final long rangeBegin;
    private final long rangeEnd;
    private final List<HostAddress> addresses;

    @JsonCreator
    public CrackerSplit(@JsonProperty("tableHandle") CrackerTableHandle tableHandle, @JsonProperty("rangeBegin") long rangeBegin, @JsonProperty("rangeEnd") long rangeEnd, @JsonProperty("addresses") List<HostAddress> addresses)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.rangeBegin = rangeBegin;
        this.rangeEnd = rangeEnd;
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
    }

    @JsonProperty
    public CrackerTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public long getRangeBegin()
    {
        return rangeBegin;
    }

    @JsonProperty
    public long getRangeEnd()
    {
        return rangeEnd;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        CrackerSplit other = (CrackerSplit) obj;
        return Objects.equals(this.tableHandle, other.tableHandle) && Objects.equals(this.rangeBegin, other.rangeBegin) && Objects.equals(this.rangeEnd, other.rangeEnd);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableHandle, rangeBegin, rangeEnd);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).add("tableHandle", tableHandle).add("rangeEnd", rangeEnd).add("rangeBegin", rangeBegin).toString();
    }
}
