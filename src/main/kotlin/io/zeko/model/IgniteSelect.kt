package io.zeko.model

class IgniteSelect: Select {
    constructor(asChar: String = "="): super("\"", asChar, true)
}
