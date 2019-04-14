package com.github.com.davidkwan.kafka;

import com.github.com.davidkwan.twitter.TwitterClient;
import com.google.common.collect.Lists;
import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());


    public TwitterProducer() {}

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        List<String> terms = Lists.newArrayList("leafs");

        // create Twitter client
        TwitterClient twitterClient = new TwitterClient();
        Client client = twitterClient.createTwitterClient(msgQueue, terms);


        // Attempts to establish a connection.
        client.connect();

        // create Kafka producer
        KafkaProducer producer = new Producer().createKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application.");
            logger.info("shutting down twitter client.");
            client.stop();
            logger.info("stopping producer.");
            producer.close();
            logger.info("shutdown complete.");
        }));


        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if(msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord("twitter_tweets", null, msg), new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e != null) {
                            logger.error("Kafka producer error.", e);
                        }
                    }
                });
            }
        }

        logger.info("End of application.");
    }

}
