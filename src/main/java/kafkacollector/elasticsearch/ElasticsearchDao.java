package kafkacollector.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import kafkacollector.common.Dao;
import kafkacollector.common.DataChannel;
import kafkacollector.exception.KafkaCollectorException;

import static kafkacollector.common.Utils.getObjectName;

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
public class ElasticsearchDao implements Dao {
    private static final Logger log = LoggerFactory.getLogger(ElasticsearchDao.class);
    private final RestClient restClient;
    private final String host;

    public ElasticsearchDao(RestClient restClient, String ip, String port){
        this.restClient = restClient;
        this.host = ip + ":" + port;
    }

    @Override
    public void collect() throws KafkaCollectorException {
    }

    @Override
    public void put() throws KafkaCollectorException {
        Map<String, Object> data = DataChannel.getKafkaMonitoringData();
        if(data.isEmpty()) return;
        ObjectMapper mapper = new ObjectMapper();
        HttpEntity httpEntity;
        try {
            Map<String, String> serialized = new HashMap<>();
            for(String objectName : data.keySet()){
                String[] splitted = getObjectName(objectName);
                serialized.put("index", String.format("%s-%s", splitted[0], LocalDate.now()));
                serialized.put("type", splitted[1]);
                String url = String.format("/%s/%s/%s", serialized.get("index"), serialized.get("type"), LocalDateTime.now());
                String body = mapper.writeValueAsString(data.get(objectName));
                log.debug(String.format("HTTP Request URL: %s, Request entity:%s", host, body));
                httpEntity = new NStringEntity(body, ContentType.APPLICATION_JSON);
                Response response = restClient.performRequest("PUT", url, Collections.<String, String>emptyMap(), httpEntity);
                log.info(String.format("HTTP Response URL: %s, Response Body: %s", host, response.toString()));
            }
        } catch (JsonProcessingException e) {
            throw new KafkaCollectorException(e);
        } catch (IOException e) {
            throw new KafkaCollectorException(e);
        }
    }
}
