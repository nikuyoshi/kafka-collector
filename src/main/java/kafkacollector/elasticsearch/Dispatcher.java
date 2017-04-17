package kafkacollector.elasticsearch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import kafkacollector.common.AbstractTaskExecutor;
import kafkacollector.common.Dao;
import kafkacollector.exception.KafkaCollectorException;

import static kafkacollector.config.AppConfigs.NUMBER_OF_THREAD_POOL;
import static kafkacollector.config.AppConfigs.TASK_TIMEOUT_MINUTES;

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
public class Dispatcher extends AbstractTaskExecutor {
    private static final Dispatcher instance = new Dispatcher();
    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);
    private Dao dao;

    private Dispatcher() {
    }

    public static Dispatcher getInstance() {
        return instance;
    }

    @Override
    public void initialize() {
        executorService = Executors.newFixedThreadPool(NUMBER_OF_THREAD_POOL);
    }

    @Override
    public void preProcess() throws KafkaCollectorException {
        dao = ElasticsearchDaoFactory.getInstance().create();
    }

    @Override
    public void process() throws KafkaCollectorException {
        List<Future<?>> futureList = new ArrayList();
        futureList.add(executorService.submit(new DispatcherTask(dao)));
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
