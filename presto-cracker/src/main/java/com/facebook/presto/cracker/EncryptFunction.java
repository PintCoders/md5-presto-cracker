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

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.security.MessageDigest;

import static com.facebook.presto.cracker.CrackerUtil.encryptPassword;
import static com.facebook.presto.cracker.CrackerUtil.getMessageDigest;

public class EncryptFunction
{
    private EncryptFunction()
    {
    }

    @Description("Returns the String representation of the MD5 encrypt of the password")
    @ScalarFunction("encrypt")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice encrypt(@SqlType(StandardTypes.VARCHAR) Slice password)
    {
        MessageDigest messageDigest = getMessageDigest();
        String encryptedPassword = encryptPassword(password.toStringUtf8(), messageDigest);
        return Slices.utf8Slice(encryptedPassword);
    }
}
