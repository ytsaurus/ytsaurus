package tech.ytsaurus.flow.examples.wordcount.model

import tech.ytsaurus.flow.row.FlowMessage
import javax.persistence.Entity

// [BEGIN stream_context]
@Entity
@FlowMessage(streamIds = ["words"])
class Word {
    var word: String? = null

    constructor()

    constructor(word: String?) {
        this.word = word
    }
}
// [END stream_context]
