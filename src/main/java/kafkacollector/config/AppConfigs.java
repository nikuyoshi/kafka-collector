package kafkacollector.config;

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
public final class AppConfigs {
    public static final String JMX_PROTOCOL = "service:jmx:rmi:///jndi/rmi://";
    public static final String APP_CONFIG_FILE_NAME = "kafka-collector.properties";
    public static final String KAFKA_SERVERS = "kafka.servers";
    public static final String ELASTICSEARCH_SERVERS = "elasticsearch.servers";
    public static final String PERIODIC_TIME_INTERVAL = "periodic.time.interval";
    public static final String IP_PORT_REGEX = "\\d{1,3}(?:\\.\\d{1,3}){3}(?::\\d{1,5})?\n";
    public static final String OBJECTNAME_TYPE_REGEX = "^([.+]*):type=([.+]*)";
    public static final String OBJECTNAME_NAME_REGEX = "^,name=([.+]*),";
    public static final String FLOAT_PATTER_REGEX = "^([+-]?\\d*\\.?\\d*)$";
    public static final int NUMBER_OF_THREAD_POOL = 5;
    public static final int TASK_TIMEOUT_MINUTES = 10;
}
