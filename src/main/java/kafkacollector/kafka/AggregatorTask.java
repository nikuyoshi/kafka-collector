package kafkacollector.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafkacollector.common.Dao;

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
public class AggregatorTask implements Runnable {
    final Logger log = LoggerFactory.getLogger(AggregatorTask.class);
    private final Dao kafkaDao;

    public AggregatorTask(Dao kafkaDao){
        this.kafkaDao = kafkaDao;
    }

    @Override
    public void run() {
        try {
            kafkaDao.collect();
            log.info("Kafka aggregation task completed!");
        } catch (Throwable t) {
            log.error(String.format("%s occurred. ", t.getClass().getCanonicalName()), t);
        }
    }
}
