package pulsar;

import org.apache.pulsar.client.api.*;

public class ReadingsProducer {
    public static final int NUM_MESSAGES = 4;

    public static void main(String[] args) throws PulsarClientException, InterruptedException {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();
        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic("persistent://public/default/temperature-readings")
                .create();

        // You can then send messages to the broker and topic you specified:
        for(int i = 0; i < NUM_MESSAGES; i++) {
            String message = (20 * i  + 50) + "";
            producer.send(message);
            System.out.println("Message produced: " + message);
        }

        producer.close();
        client.close();
        System.out.println("Demo finished.");
    }
}
