/*
 * Copyright 2019 Eclipse.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vertx.sqlclient.mapper;

import io.vertx.core.buffer.Buffer;
import io.vertx.sqlclient.Row;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.temporal.Temporal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Class Dissector.
 * Scans a class identifying all its property getters and setters.
 * 
 * @author <a href="mailto:eduard.catala@gmail.com">Eduard Català</a>
 */
public class ClassDissector {

    private static final String GET_PREFIX = "get";
    private static final String SET_PREFIX = "set";
    private static final String IS_PREFIX = "is";
    private final Map<Class, Method> ROW_TYPEMAP = new HashMap<>();

    public ClassDissector() {
        try {
            ROW_TYPEMAP.put(String.class, Row.class.getMethod("getString", int.class));
            ROW_TYPEMAP.put(Integer.class, Row.class.getMethod("getInteger", int.class));
            ROW_TYPEMAP.put(int.class, ROW_TYPEMAP.get(Integer.class));
            ROW_TYPEMAP.put(Short.class, Row.class.getMethod("getShort", int.class));
            ROW_TYPEMAP.put(short.class, ROW_TYPEMAP.get(Short.class));
            ROW_TYPEMAP.put(Long.class, Row.class.getMethod("getLong", int.class));
            ROW_TYPEMAP.put(long.class, ROW_TYPEMAP.get(Long.class));
            ROW_TYPEMAP.put(Boolean.class, Row.class.getMethod("getBoolean", int.class));
            ROW_TYPEMAP.put(boolean.class, ROW_TYPEMAP.get(Long.class));
            ROW_TYPEMAP.put(Double.class, Row.class.getMethod("getDouble", int.class));
            ROW_TYPEMAP.put(double.class, ROW_TYPEMAP.get(Double.class));
            ROW_TYPEMAP.put(Float.class, Row.class.getMethod("getFloat", int.class));
            ROW_TYPEMAP.put(float.class, ROW_TYPEMAP.get(Float.class));
            ROW_TYPEMAP.put(OffsetDateTime.class, Row.class.getMethod("getOffsetDateTime", int.class));
            ROW_TYPEMAP.put(OffsetTime.class, Row.class.getMethod("getOffsetTime", int.class));
            ROW_TYPEMAP.put(Buffer.class, Row.class.getMethod("getBuffer", int.class));
            ROW_TYPEMAP.put(BigDecimal.class, Row.class.getMethod("getBigDecimal", int.class));
            ROW_TYPEMAP.put(LocalTime.class, Row.class.getMethod("getLocalTime", int.class));
            ROW_TYPEMAP.put(LocalDateTime.class, Row.class.getMethod("getLocalDateTime", int.class));
            ROW_TYPEMAP.put(LocalDate.class, Row.class.getMethod("getLocalDate", int.class));
            ROW_TYPEMAP.put(Temporal.class, Row.class.getMethod("getTemporal", int.class));
            ROW_TYPEMAP.put(UUID.class, Row.class.getMethod("getUUID", int.class));
            ROW_TYPEMAP.put(Object.class, Row.class.getMethod("getValue", int.class));
        } catch (NoSuchMethodException | SecurityException e) {

            // Silenced
        }
    }

    /**
     * Returns a map of property names to its setters/getters
     *
     * The property names are generated from method names not from class fields.
     *
     * @param clazz Class to be dissected
     * @return map of Dissection object by propety name
     */
    public Map<String, Dissection> dissect(Class clazz) {

        Method[] methods = getPublicMethods(clazz);
        Map<String, Dissection> dissected = new HashMap<>();

        for (int i = 0; i < methods.length; i++) {
            Method method = methods[i];

            if (method == null) {
                continue;
            }

            String name = method.getName();
            int argCount = method.getParameterTypes().length;
            Class resultType = method.getReturnType();

            if (name.length() <= 3 && !name.startsWith(IS_PREFIX)) { // prune invalid
                continue;
            }

            Dissection dissection;

            if (argCount == 0) {
                if (name.startsWith(GET_PREFIX)) { // Simple getter
                    name = name.substring(3);
                    dissection = getOrCreate(name, dissected);
                    dissection.getter = method;
                    dissection.getterType = resultType;

                } else if (resultType == boolean.class && name.startsWith(IS_PREFIX)) { // Boolean getter
                    name = name.substring(2);
                    dissection = getOrCreate(name, dissected);
                    dissection.getter = method;
                    dissection.getterType = resultType;
                }
            } else if (argCount == 1) {
                if (void.class.equals(resultType) && name.startsWith(SET_PREFIX)) { // Simple setter
                    name = name.substring(3);
                    dissection = getOrCreate(name, dissected);
                    dissection.setter = method;
                    dissection.setterType = method.getParameterTypes()[0];
                    dissection.rowGetter = ROW_TYPEMAP.get(dissection.setterType);
                }
            }
        }

        return dissected;

    }

    private Dissection getOrCreate(String propertyName, Map<String, Dissection> dissected) {
        propertyName = propertyName.toLowerCase();
        Dissection d = dissected.get(propertyName);
        if (d == null) {
            d = new Dissection();
            dissected.put(propertyName, d);
        }
        return d;
    }

    private Method[] getPublicMethods(Class clazz) {

        if (!Modifier.isPublic(clazz.getModifiers())) {
            return new Method[0];
        }

        Method[] result = clazz.getMethods();
        for (int i = 0; i < result.length; i++) {
            Method method = result[i];

            if (!Modifier.isPublic(method.getModifiers()) || method.getDeclaringClass().equals(Object.class)) {
                result[i] = null;
            }

        }

        return result;

    }

    /**
     * Utility class containing class properties access methods linked to Row
     * getter method.
     */
    public static class Dissection {

        private Method getter;
        private Method setter;
        private Class getterType;
        private Class setterType;
        private Method rowGetter;

        public Method getGetter() {
            return getter;
        }

        public Method getSetter() {
            return setter;
        }

        public Class getGetterType() {
            return getterType;
        }

        public Class getSetterType() {
            return setterType;
        }

        public Method getRowGetter() {
            return rowGetter;
        }

    }

}
