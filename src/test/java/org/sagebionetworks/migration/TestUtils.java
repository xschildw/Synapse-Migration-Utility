package org.sagebionetworks.migration;

import java.lang.reflect.Field;

public class TestUtils {
    public static void setPrivateField(Object target, String fieldName, Object value){
        try {
            Field privateField = target.getClass().getDeclaredField(fieldName);
            privateField.setAccessible(true);
            privateField.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
