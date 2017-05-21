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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class CrackerUtil
{
    public static final String PASSWORD_CHARS = "0123456789abcdefghijklmnopqrstuvwxyz";    // Possible Password symbol (NUMBER(0~9) + CHARACTER(A to Z))
    public static final int PASSWORD_LEN = 6;
    public static final long TOTAL_PASSWORD_RANGE_SIZE = (long) Math.pow(PASSWORD_CHARS.length(), PASSWORD_LEN);

    private CrackerUtil()
    {
    }

    public static MessageDigest getMessageDigest()
    {
        try {
            return MessageDigest.getInstance("MD5");
        }
        catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            throw new RuntimeException("Cannot use MD5 Library:" + e.getMessage());
        }
    }

    public static String encryptPassword(String password, MessageDigest messageDigest)
    {
        messageDigest.update(password.getBytes());
        byte[] hashedValue = messageDigest.digest();
        return byteToHexString(hashedValue);
    }

    public static String byteToHexString(byte[] bytes)
    {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            String hex = Integer.toHexString(0xFF & bytes[i]);
            if (hex.length() == 1) {
                builder.append('0');
            }
            builder.append(hex);
        }
        return builder.toString();
    }

    public static void transformDecToBase36(long numInDec, int[] numArrayInBase36)
    {
        /** COMPLETE **/
    }

    public static void getNextCandidate(int[] candidateChars)
    {
        /** OPTIONAL **/
    }

    public static String transformIntoStr(int[] chars)
    {
        /** COMPLETE **/
        return null;
    }
}
