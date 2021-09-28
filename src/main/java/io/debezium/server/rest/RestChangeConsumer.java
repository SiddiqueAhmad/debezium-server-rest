package io.debezium.server.rest;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.CustomConsumerBuilder;

/**
 * Implementation of the consumer that delivers the messages rest end point.
 *
 * @author Siddique Ahmad
 * @author Usama qazi
 *
 */
@Named("rest")
@Dependent
public class RestChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RestChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.rest.";
    private static final String PROP_ENDPOINT_NAME = PROP_PREFIX + "endpoint";

    private String endpoint;

    private CloseableHttpClient client = null;

    @Inject
    @CustomConsumerBuilder
    Instance<CloseableHttpClient> customClient;

    @PostConstruct
    void connect() {
        if (customClient.isResolvable()) {
            client = customClient.get();
            LOGGER.info("Obtained custom configured RestClient '{}'", client);
            return;
        }

        final Config config = ConfigProvider.getConfig();
        endpoint = config.getValue(PROP_ENDPOINT_NAME, String.class);

        client = HttpClients.createDefault();
        LOGGER.info("Using default HttpClient '{}'", client);
    }

    @PreDestroy
    void close() {
        try {
            client.close();
        }
        catch (Exception e) {
            LOGGER.warn("Exception while closing http client: ", e);
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        for (ChangeEvent<Object, Object> record : records) {
            LOGGER.trace("Received event '{}'", record);

            HttpPost post = new HttpPost(endpoint);

            try {
                post.setEntity(new StringEntity(record.value().toString() + record.destination() + record.key()));
            } catch (UnsupportedEncodingException e) {
                LOGGER.warn("Unsupported Encoding Exception while sending post http client: ", e);
            }

            try {
                client.execute(post);
            } catch (IOException e) {
                LOGGER.trace("IO Exception while sending post http client: ", e);
            }
            committer.markProcessed(record);
        }
        committer.markBatchFinished();
    }
}
