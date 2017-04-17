package kafkacollector.common;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import kafkacollector.exception.KafkaCollectorException;

import static kafkacollector.config.AppConfigs.OBJECTNAME_NAME_REGEX;
import static kafkacollector.config.AppConfigs.OBJECTNAME_TYPE_REGEX;

/**
 * Copyright 2017 Hiroki Uchida
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
final public class Utils {

    private static final Pattern typePattern = Pattern.compile(OBJECTNAME_TYPE_REGEX);
    private static final Pattern namePattern = Pattern.compile(OBJECTNAME_NAME_REGEX);
    private Utils() {
    }

    /**
     * Read a properties file from the given path
     *
     * @param filename The path of the file to read
     */
    public static Properties loadProps(String filename) throws KafkaCollectorException {
        Properties props = new Properties();
        try (InputStream propStream = new FileInputStream(filename)) {
            props.load(propStream);
        } catch (FileNotFoundException e) {
            throw new KafkaCollectorException(String.format("%s occurred. Path: %s", e.getClass().getCanonicalName(), filename), e);
        } catch (IOException e) {
            throw new KafkaCollectorException(String.format("%s occurred. Path: %s", e.getClass().getCanonicalName(), filename), e);
        }
        return props;
    }

    public static boolean checkString(String str, String regex) {
        return str.matches(regex);
    }

    public static Map<String, String> getObjectName(String objectName){
        String replaced = objectName.toLowerCase().replace(",", ".").replace(" ", "");
        String[] splitted = replaced.split(":");
        Map<String, String> result = new HashMap<>();
        result.put("index", splitted[0]);
        result.put("type", splitted[1]);
        return result;
    }
}
