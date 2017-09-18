package com.adeotek.java.firebirdsql;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class Helpers {
    public static boolean isStringEmptyOrNull(String input, boolean trimInput) {
        if(input == null) {
            return true;
        }
        if(trimInput) {
            return input.trim().length()==0;
        }
        return input.length()==0;
    }//isStringEmptyOrNull

    public static boolean isStringEmptyOrNull(String input) {
        return isStringEmptyOrNull(input, true);
    }//isStringEmptyOrNull

    public static String getDuration(long sts) {
        return ((double)(System.currentTimeMillis()-sts)/1000) + " sec.";
    }//getDuration

    public static JsonElement getJsonElement(JsonObject jsonObject, String memberName) {
        if (jsonObject == null || !jsonObject.has(memberName)) {
            return null;
        }
        return jsonObject.get(memberName);
    }//getJsonElement

    public static int getJsonElementAsInt(JsonObject jsonObject, String memberName, int defaultValue) {
        if (jsonObject == null || !jsonObject.has(memberName)) {
            return defaultValue;
        }
        try {
            return jsonObject.get(memberName).getAsInt();
        } catch (Exception se) {
            // silent error
            return defaultValue;
        }
    }//getJsonElementAsInt

    public static int getJsonElementAsInt(JsonObject jsonObject, String memberName) {
        return getJsonElementAsInt(jsonObject, memberName, 0);
    }

    public static long getJsonElementAsLong(JsonObject jsonObject, String memberName, long defaultValue) {
        if (jsonObject == null || !jsonObject.has(memberName)) {
            return defaultValue;
        }
        try {
            return jsonObject.get(memberName).getAsLong();
        } catch (Exception se) {
            // silent error
            return defaultValue;
        }
    }//getJsonElementAsLong

    public static long getJsonElementAsLong(JsonObject jsonObject, String memberName) {
        return getJsonElementAsLong(jsonObject, memberName, 0);
    }

    public static String getJsonElementAsString(JsonObject jsonObject, String memberName, String defaultValue) {
        if (jsonObject == null || !jsonObject.has(memberName)) {
            return defaultValue;
        }
        try {
            return jsonObject.get(memberName).getAsString();
        } catch (Exception se) {
            // silent error
            return defaultValue;
        }
    }//getJsonElementAsString

    public static String getJsonElementAsString(JsonObject jsonObject, String memberName) {
        return getJsonElementAsString(jsonObject, memberName, null);
    }//getJsonElementAsString

    public static String hash(String salt, String algorithm, boolean noTime) throws Exception {
        StringBuffer sb = new StringBuffer();
        try {
            MessageDigest mDigest = MessageDigest.getInstance(algorithm);
            String input = "";
            if (!isStringEmptyOrNull(salt)) {
                input += salt;
            }
            if (!noTime) {
                input += Long.valueOf(System.currentTimeMillis()).toString();
            }
            if (isStringEmptyOrNull(input)) {
                return null;
            }
            byte[] result = mDigest.digest(input.getBytes("UTF-8"));
            for (int i = 0; i < result.length; i++) {
                sb.append(Integer.toString((result[i] & 0xff) + 0x100, 16).substring(1));
            }
        } catch (NoSuchAlgorithmException nsae) {
            throw new Exception("NoSuchAlgorithmException: " + nsae.getMessage());
        } catch (UnsupportedEncodingException uee) {
            throw new Exception("UnsupportedEncodingException: " + uee.getMessage());
        }
        return sb.toString();
    }//hash

    public static String uidHash(String salt) throws Exception {
        return hash(salt, "SHA1", false);
    }//uidHash

    public static String sha1(String input) throws Exception {
        return hash(input, "SHA1", true);
    }//sha1

    public static String md5(String input) throws Exception {
        return hash(input, "MD5", true);
    }//md5

    public static byte[] readBytes(InputStream inputStream) {
        // this dynamically extends to take the bytes you read
        ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
        // this is storage overwritten on each iteration with bytes
        int bufferSize = 1024;
        byte[] buffer = new byte[bufferSize];
        // we need to know how may bytes were read to write them to the byteBuffer
        int len;
        try {
            while ((len = inputStream.read(buffer)) != -1) {
                byteBuffer.write(buffer, 0, len);
            }
        } catch (Exception e) {
            return null;
        }
        // and then we can return your byte array.
        return byteBuffer.toByteArray();
    }//readBytes

    public static String inputStreamToString(InputStream inputStream) {
        // this dynamically extends to take the bytes you read
        ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
        // this is storage overwritten on each iteration with bytes
        int bufferSize = 1024;
        byte[] buffer = new byte[bufferSize];
        // we need to know how may bytes were read to write them to the byteBuffer
        int len;
        try {
            while ((len = inputStream.read(buffer)) != -1) {
                byteBuffer.write(buffer, 0, len);
            }
        } catch (Exception e) {
            return null;
        }
        // StandardCharsets.UTF_8.name() > JDK 7
        try {
            return byteBuffer.toString(StandardCharsets.UTF_8.name());
        } catch(UnsupportedEncodingException use) {
            return byteBuffer.toString();
        }
    }//inputStreamToString

    public static InputStream stringToInputStream(String input) {

        byte[] content;
        try {
            content = input.getBytes(StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException use) {
            content = input.getBytes();
        }
        return new ByteArrayInputStream(content);
    }//stringToInputStream
}//Helpers
