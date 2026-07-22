package tech.ytsaurus.flow.examples.wordcount.model

import javax.persistence.Entity

@Entity
class Word {
    var word: String? = null

    constructor()

    constructor(word: String?) {
        this.word = word
    }
}
