package kafkacollector.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

import kafkacollector.exception.KafkaCollectorException;

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
public class AppConfigLoader {
    private static final Logger log = LoggerFactory.getLogger(AppConfigLoader.class);
    public static void load() throws KafkaCollectorException {
        Properties prop = System.getProperties();
        try (InputStream input = AppConfigLoader.class.getClassLoader().getResourceAsStream(AppConfigs.APP_CONFIG_FILE_NAME)){
            prop.load(input);
            System.setProperties(prop);
        } catch (IOException e) {
            throw new KafkaCollectorException(String.format("%s occurred. Properties: %s", e.getClass().getCanonicalName(), prop.toString()), e);
        }
        Arrays.stream(prop.toString().split(",")).forEach(str -> {
            log.info(String.format("%s", str));
        });
    }
}
