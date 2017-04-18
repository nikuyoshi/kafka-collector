package kafkacollector.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import kafkacollector.common.Dao;
import kafkacollector.common.DaoFactory;
import kafkacollector.common.Utils;
import kafkacollector.exception.KafkaCollectorException;

import static kafkacollector.config.AppConfigs.ELASTICSEARCH_SERVERS;
import static kafkacollector.config.AppConfigs.IP_PORT_REGEX;

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
public class ElasticsearchDaoFactory implements DaoFactory {
    private static final Logger log = LoggerFactory.getLogger(ElasticsearchDaoFactory.class);
    private static final ElasticsearchDaoFactory instance = new ElasticsearchDaoFactory();

    private ElasticsearchDaoFactory(){
    }

    public static ElasticsearchDaoFactory getInstance() {
        return instance;
    }

    @Override
    public <T extends Dao> T create() throws KafkaCollectorException{
        Properties prop = System.getProperties();
        String[] elasticsearchServers = prop.getProperty(ELASTICSEARCH_SERVERS).split(",");
        HttpHost[] httpHostList = new HttpHost[elasticsearchServers.length];
        int i = 0, port = 0;
        String ip = null;
        for (String server : elasticsearchServers) {
            if (Utils.checkString(server, IP_PORT_REGEX)) {
                log.error(String.format("Illegal URI format: %s", server));
                break;
            }
            ip = server.split(":")[0];
            port = Integer.valueOf(server.split(":")[1]);
            HttpHost httpHost = new HttpHost(ip, port, "Http");
            httpHostList[i++] = httpHost;
        }

        RestClient client = RestClient.builder(httpHostList).build();
        for(HttpHost host : httpHostList){
            log.info(String.format("Connection is successful. %s", host.toString()));
        }
        Dao dao = new ElasticsearchDao(client, ip, String.valueOf(port));
        return (T) dao;
    }
}
