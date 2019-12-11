package pulsar;

import org.apache.pulsar.client.api.*;

public class StringConsumer {

    public static void main(String[] args) throws PulsarClientException, InterruptedException {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();

        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topicsPattern("persistent://public/default/f2c-func-.*")
                .subscriptionName("f2c-subscription")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();

        // receiving messages asynchronously
        for(int i = 0; i < ReadingsProducer.NUM_MESSAGES; i++) {
            Message msg = consumer.receive();
            System.out.printf("!!!! Message received: %s\n\n", msg.getValue());
            consumer.acknowledge(msg);
        }

        consumer.close();
        client.close();
        System.out.println("Demo finished.");


    }
}
