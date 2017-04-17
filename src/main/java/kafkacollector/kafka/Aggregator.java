package kafkacollector.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import kafkacollector.common.AbstractTaskExecutor;
import kafkacollector.common.Dao;
import kafkacollector.common.TaskExecutor;
import kafkacollector.common.Utils;
import kafkacollector.exception.KafkaCollectorException;

import static kafkacollector.config.AppConfigs.IP_PORT_REGEX;
import static kafkacollector.config.AppConfigs.KAFKA_SERVERS;
import static kafkacollector.config.AppConfigs.NUMBER_OF_THREAD_POOL;
import static kafkacollector.config.AppConfigs.TASK_TIMEOUT_MINUTES;

/**
 * Copyright 2017 Hiroki Uchida
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class Aggregator extends AbstractTaskExecutor{
    private static final TaskExecutor instance = new Aggregator();
    private static final Logger log = LoggerFactory.getLogger(Aggregator.class);
    private final List<Dao> kafkaDaoList = new ArrayList<>();

    private Aggregator() {
    }

    public static TaskExecutor getInstance() {
        return instance;
    }

    @Override
    public void initialize() {
        executorService = Executors.newFixedThreadPool(NUMBER_OF_THREAD_POOL);
        kafkaDaoList.clear();
    }

    @Override
    public void preProcess() throws KafkaCollectorException{
        Properties prop = System.getProperties();
        String[] kafkaServers = prop.getProperty(KAFKA_SERVERS).split(",");
        for(String host : kafkaServers){
            if (Utils.checkString(host, IP_PORT_REGEX)) {
                log.warn(String.format("Illegal URI format: %s", host));
                break;
            }
            String ip = host.split(":")[0];
            String port = host.split(":")[1];
            Dao kafkaDao = KafkaDaoFactory.getInstance().setData(ip, port).create();
            this.kafkaDaoList.add(kafkaDao);
        }
    }

    @Override
    public void process() throws KafkaCollectorException{
        List<Future<?>> futureList = new ArrayList();
        for(Dao kafkaDao : kafkaDaoList){
            futureList.add(executorService.submit(new AggregatorTask(kafkaDao)));
        }
        try {
            for(Future<?> future : futureList) {
                future.get(TASK_TIMEOUT_MINUTES, TimeUnit.MINUTES);
            }
        } catch (InterruptedException e) {
            throw new KafkaCollectorException(e);
        } catch (ExecutionException e) {
            throw new KafkaCollectorException(e);
        } catch (TimeoutException e){
            throw new KafkaCollectorException(e);
        } catch (Throwable t){
            throw new KafkaCollectorException(t);
        }
    }

    @Override
    public void postProcess() {
    }

    @Override
    public void terminate() {
        try {
            List<?> tasks = executorService.shutdownNow();
            tasks.stream().findAny().ifPresent(list -> log.warn(String.format("Force shutdown tasks: %s", list)));
        } catch (Throwable t){
            log.error("Connection close error occurred. ", t);
            throw t;
        }
    }
}
