package pulsar.function;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

/**
 * The classic word count example done using pulsar functions
 * Each input message is a sentence that split into words and each word counted.
 * The built in counter state is used to keep track of the word count in a
 * persistent and consistent manner.
 */
public class ByTwoFunction implements Function<String, String> {
    @Override
    public String process(String input, Context context) {
        try {
            double doubleReading = Double.parseDouble(input);
            return 2*doubleReading + "";
        } catch (NumberFormatException ex) {
            context.getLogger().error("Error parsing reading as double: {}", input);
        }
        return null;
    }
}
