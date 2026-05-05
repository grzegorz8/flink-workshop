package com.xebia.flink.workshop.utils;

import java.util.Random;

public class RandomStringGenerator {

    private static final String CHARSET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final Random RANDOM = new Random();

    public static String generateRandomString(int length) {
        StringBuilder sb = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            int index = RANDOM.nextInt(CHARSET.length());
            sb.append(CHARSET.charAt(index));
        }

        return sb.toString();
    }

}
