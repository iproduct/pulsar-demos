package pulsar;

import org.apache.pulsar.client.api.*;

public class PulsarBasicClient {

    public static void main(String[] args) throws PulsarClientException, InterruptedException {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();
        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic("my-topic")
                .create();

        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic("my-topic")
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();

        // receiving messages asynchronously
        for(int i = 0; i < 4; i++) {
            consumer.receiveAsync().thenAccept(msg -> {
                System.out.printf("!!!! Message received: %s\n\n", msg.getValue());
                try {
                    consumer.acknowledge(msg);
                } catch (PulsarClientException e) {
                    e.printStackTrace();
                }
            });
        }

        // You can then send messages to the broker and topic you specified:
        producer.send("My message 1");
        producer.sendAsync("my-async-message 1").thenAccept(msgId -> {
            System.out.printf("Message with ID %s successfully sent \n\n", msgId);
        });
        producer.send("My message 2");
        producer.send("My message 3");
        producer.send("My message 4");


        Thread.sleep(10000);
        producer.close();
        consumer.close();
        client.close();
        System.out.println("Demo finished.");


    }
}
