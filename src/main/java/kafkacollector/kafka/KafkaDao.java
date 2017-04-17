package kafkacollector.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import kafkacollector.common.Dao;
import kafkacollector.common.DataChannel;
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
public class KafkaDao implements Dao {
    private static final Logger log = LoggerFactory.getLogger(KafkaDao.class);
    private final JmxClient jmxClient;

    public KafkaDao(JmxClient jmxClient){
        this.jmxClient = jmxClient;
    }

    @Override
    public void collect() throws KafkaCollectorException{
        this.collectJmxMetricsData();
    }

    @Override
    public void put() throws KafkaCollectorException {

    }

    private void collectJmxMetricsData() throws KafkaCollectorException {
        try {
            jmxClient.connect();
            MBeanServerConnection mBeanServerConnection = jmxClient.getJmxConnector().getMBeanServerConnection();
            Set<ObjectName> objectNames = mBeanServerConnection.queryNames(null, null);
            for(ObjectName objectName : objectNames) {
                if(objectName.toString().matches("java.lang:type=MemoryPool,name=Metaspace")) break;
                MBeanInfo mbeanInfo = mBeanServerConnection.getMBeanInfo(objectName);
                MBeanAttributeInfo[] mBeanAttributeInfoList = mbeanInfo.getAttributes();
                Map<String, Object> map = DataChannel.getKafkaMonitoringData();
                Map<String, String> attributeInfoMap = new HashMap<>();
                for(MBeanAttributeInfo info : mBeanAttributeInfoList){
                    String attributeName = info.getName();
                    String attributeValues = mBeanServerConnection.getAttribute(objectName, info.getName()).toString();
                    attributeInfoMap.put(attributeName, attributeValues);
                }
                map.put(objectName.toString(), attributeInfoMap);
            }
        } catch (InstanceNotFoundException e) {
            throw new KafkaCollectorException(e);
        } catch (ReflectionException e) {
            throw new KafkaCollectorException(e);
        } catch (IOException e) {
            throw new KafkaCollectorException(e);
        } catch (Throwable t){
            throw new KafkaCollectorException(t);
        } finally {
            try {
                jmxClient.close();
            } catch (Throwable t){
                log.error("Connection close error occurred. ", t);
            }
        }
    }
}
