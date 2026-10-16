package tech.ytsaurus.flow.examples.wordcount;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

// [BEGIN word_count_application]
@SpringBootApplication
public class WordCountApplication {
    public static void main(String[] args) {
        new SpringApplicationBuilder(WordCountApplication.class)
                .run(args);
    }
}
// [END word_count_application]
