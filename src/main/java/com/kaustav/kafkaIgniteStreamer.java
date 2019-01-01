package com.kaustav;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class kafkaIgniteStreamer {
    private static final String CACHE_NAME = "telstraCAche001";

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9777");
        props.put("group.id", "test2");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("telstra_topic03"));



        Ignition.setClientMode(true);

        System.out.println("1st first chkpt");

        // cluster tcp configuration
        final TcpDiscoverySpi tcpDiscoverySpi = new TcpDiscoverySpi();
        final TcpDiscoveryKubernetesIpFinder tcpDiscoveryKubernetesIpFinder = new com.kaustav.TcpDiscoveryKubernetesIpFinder();


        // need to be changed when it come to real cluster configuration
        //tcpDiscoveryKubernetesIpFinder.registerAddresses(tcpDiscoveryKubernetesIpFinder.getRegisteredAddresses());
        tcpDiscoveryKubernetesIpFinder.getRegisteredAddresses();
        System.out.println("2nd chkpt");

        tcpDiscoverySpi.setIpFinder(tcpDiscoveryKubernetesIpFinder);



        IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
        igniteConfiguration.setDiscoverySpi(new TcpDiscoverySpi());
        igniteConfiguration.setIgniteInstanceName("client1");

        //igniteConfiguration.setClientMode(true);



        final Ignite ignite = Ignition.start(igniteConfiguration);

        //Ignite ignite = Ignition.start("/home/system/IGNITE/APPS/Telstra_Network_Disruptions_Prediction/telstra-kafka-ignite-2_7-streamer/config/example-kube-rbac.xml");
        //Ignite ignite = Ignition.start(config("client1", true, false));
            //if (!ExamplesUtils.hasServerNodes(ignite))
            //return;

            // The cache is configured with sliding window holding 1 second of the streaming data.
            //IgniteCache<AffinityUuid, String> stmCache = ignite.getOrCreateCache(CacheConfig.wordCache());



        CacheConfiguration<Long, String> cfg = new CacheConfiguration<>(CACHE_NAME);

            // Index key and value.
        cfg.setIndexedTypes(Long.class, String.class);

            // Auto-close cache at the end of the example.
        IgniteCache<Long, String> stmCache = ignite.getOrCreateCache(cfg);



        // Get the data streamer reference and stream data.
        try (IgniteDataStreamer<Long, String> stmr1 = ignite.dataStreamer(stmCache.getName()))



        {
            stmr1.allowOverwrite(true);

            consumer.poll(0);
// Now there is heartbeat and consumer is "alive"
            consumer.seekToBeginning(consumer.assignment());
            // Stream entries.
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                //Map<String, String> entries = new HashMap<>();
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Before Ignite");
                    stmCache.put(Long.parseLong(record.key()),(String)record.value());
                    //stmr1.addData((String)record.key(),(String)record.value());
                    //record.key();
                    System.out.printf("partition=%d ,offset = %d, key = %s, value = %s", record.partition() ,record.offset(), record.key(), record.value());
                    System.out.println("\n");
                    String a=stmCache.get(Long.parseLong(record.key()));

                    //System.out.printf(a);

                }
            }
        }

    }


}