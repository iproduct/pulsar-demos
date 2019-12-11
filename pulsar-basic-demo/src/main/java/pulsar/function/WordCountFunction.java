package pulsar.function;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import java.util.Arrays;

/**
 * The classic word count example done using pulsar functions
 * Each input message is a sentence that split into words and each word counted.
 * The built in counter state is used to keep track of the word count in a
 * persistent and consistent manner.
 */
public class WordCountFunction implements Function<String, Void> {
    @Override
    public Void process(String input, Context context) {
        Arrays.asList(input.split("\\s+")).forEach(word -> context.incrCounter(word, 1));
        return null;
    }
}
