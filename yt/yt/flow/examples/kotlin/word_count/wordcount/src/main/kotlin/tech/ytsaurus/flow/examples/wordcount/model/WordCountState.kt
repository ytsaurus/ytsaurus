package tech.ytsaurus.flow.examples.wordcount.model

import javax.persistence.Entity

@Entity
class WordCountState {
    var word: String? = null
    var count: Long = 0

    constructor()

    constructor(word: String?, count: Long) {
        this.word = word
        this.count = count
    }

    override fun toString(): String {
        return "WordCount{" +
            "word='" + word + '\'' +
            ", count=" + count +
            '}'
    }
}
