package kafkacollector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import kafkacollector.config.AppConfigLoader;
import kafkacollector.elasticsearch.Dispatcher;
import kafkacollector.exception.KafkaCollectorException;
import kafkacollector.kafka.Aggregator;

import static kafkacollector.config.AppConfigs.PERIODIC_TIME_INTERVAL;

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
public class App {
    public static void main(String... args) {
        final Logger log = LoggerFactory.getLogger(App.class);
        try {
            AppConfigLoader.load();
        } catch (KafkaCollectorException e) {
            log.error("Application config loading error. Please confirm your configuration.", e);
            System.exit(1);
        }
        long interval = Long.valueOf(System.getProperty(PERIODIC_TIME_INTERVAL));
        while(true){
            try {
                Aggregator.getInstance().execute();
                Dispatcher.getInstance().execute();
                TimeUnit.SECONDS.sleep(interval);
            } catch (Throwable t) {
                log.error("Fatal Application Error occurred. ", t);
                System.exit(1);
            }
        }
    }
}
