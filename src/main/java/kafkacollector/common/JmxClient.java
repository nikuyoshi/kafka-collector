package kafkacollector.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import kafkacollector.exception.KafkaCollectorException;

import static com.google.common.base.Preconditions.checkNotNull;
import static kafkacollector.config.AppConfigs.JMX_PROTOCOL;

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
final public class JmxClient {
    private static final Logger log = LoggerFactory.getLogger(JmxClient.class);
    private final String ip;
    private final String port;
    private JMXConnector jmxConnector;
    public JmxClient(String ip, String port){
        this.ip = ip;
        this.port = port;
    }

    public JMXConnector getJmxConnector(){
        return this.jmxConnector;
    }

    public JMXConnector connect() throws KafkaCollectorException {
        StringBuilder serviceUrl = new StringBuilder()
                .append(JMX_PROTOCOL)
                .append(this.ip)
                .append(":")
                .append(this.port)
                .append("/jmxrmi");
        try {
            JMXServiceURL jmxServiceURL = new JMXServiceURL(serviceUrl.toString());
            jmxConnector = JMXConnectorFactory.connect(jmxServiceURL);
            log.info(String.format("Connection is successful. %s", jmxConnector.toString()));
        } catch (MalformedURLException e) {
            throw new KafkaCollectorException(String.format("%s occurred. URL: %s", e.getClass().getCanonicalName(), serviceUrl), e);
        } catch (IOException e) {
            throw new KafkaCollectorException(String.format("%s occurred. URL: %s", e.getClass().getCanonicalName(), serviceUrl), e);
        }
        return jmxConnector;
    }

    public void close() throws KafkaCollectorException {
        checkNotNull(jmxConnector);
        try {
            jmxConnector.close();
            log.info(String.format("Disconection is successful. %s", jmxConnector.toString()));
        } catch (IOException e) {
            throw new KafkaCollectorException(String.format("Cannot close connection. "), e);
        }
    }
}
