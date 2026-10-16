package tech.ytsaurus.flow.examples.wordcount.model;

import javax.persistence.Entity;

import tech.ytsaurus.flow.row.FlowMessage;

// [BEGIN stream_context]
@Entity
@FlowMessage(streamIds = {"words"})
public class Word {
    private String word;

    public Word() {
    }

    public Word(String word) {
        this.word = word;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }
}
// [END stream_context]
