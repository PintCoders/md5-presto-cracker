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

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class CrackerRecordSet implements RecordSet
{
    private final long candidateRangeBegin;
    private final long candidateRangeSize;

    public CrackerRecordSet(long candidateRangeBegin, long candidateRangeSize)
    {
        this.candidateRangeBegin = candidateRangeBegin;
        this.candidateRangeSize = candidateRangeSize;
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return ImmutableList.of(CrackerConsts.COLUMN_TYPE);
    }

    @Override
    public RecordCursor cursor()
    {
        return new CrackerRecordCursor(candidateRangeBegin, candidateRangeSize);
    }
}
