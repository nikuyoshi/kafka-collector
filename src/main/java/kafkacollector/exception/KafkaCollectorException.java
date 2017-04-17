package kafkacollector.exception;

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
public class KafkaCollectorException extends Exception {

    private static final long serialVersionUID = -5243933316709548264L;

    public KafkaCollectorException(String message, Throwable cause) {
        super(message, cause);
    }

    public KafkaCollectorException(String message) {
        super(message);
    }

    public KafkaCollectorException(Throwable cause) {
        super(cause);
    }

    public KafkaCollectorException() {
        super();
    }
}
