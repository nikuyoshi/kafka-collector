package kafkacollector.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafkacollector.common.Dao;
import kafkacollector.common.DaoFactory;
import kafkacollector.common.JmxClient;
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
public class KafkaDaoFactory implements DaoFactory{
    private final static Logger log = LoggerFactory.getLogger(KafkaDaoFactory.class);
    private static final KafkaDaoFactory instance = new KafkaDaoFactory();
    private String ip;
    private String port;

    private KafkaDaoFactory(){
    }

    public static KafkaDaoFactory getInstance(){
        return instance;
    }

    public KafkaDaoFactory setData(String ip, String port){
        this.ip = ip;
        this.port = port;
        return instance;
    }

    @Override
    public <T extends Dao> T create() throws KafkaCollectorException{
        Dao dao = new KafkaDao(new JmxClient(this.ip, this.port), this.ip, this.port);
        return (T) dao;
    }
}
