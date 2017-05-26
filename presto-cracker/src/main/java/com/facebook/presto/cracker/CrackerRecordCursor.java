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
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public class CrackerRecordCursor implements RecordCursor
{
    private long candidateRangeSize;
    private boolean closed = false;

    private int[] candidateChars = new int[CrackerUtil.PASSWORD_LEN];
    private String candidatePassword;

    private long count;

    public CrackerRecordCursor(long candidateRangeBegin, long candidateRangeSize)
    {
        this.candidateRangeSize = candidateRangeSize;
        /** OPTIONAL **/

        CrackerUtil.transformDecToBase36(candidateRangeBegin, candidateChars);
        candidatePassword = CrackerUtil.transformIntoStr(candidateChars);
        count = 1;
    }

    @Override
    public long getTotalBytes()
    {
        return 0;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        return CrackerConsts.COLUMN_TYPE;
    }

    @Override
    public synchronized boolean advanceNextPosition()
    {
        /** COMPLETE **/
        if (count == candidateRangeSize) {
            return false;
        }

        CrackerUtil.getNextCandidate(candidateChars);
        candidatePassword = CrackerUtil.transformIntoStr(candidateChars);
        count++;

        return true;
    }

    @Override
    public boolean getBoolean(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice getSlice(int field)
    {
        return Slices.utf8Slice(candidatePassword);
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        return false;
    }

    @Override
    public void close()
    {
        closed = true;
    }
}
