package by.heap.storage.s3

fun Array<String>.parse(index: Int, name: String): String {
    val param = this.getOrNull(index) ?: throw RuntimeException("$name not provided.")
    if (param.isEmpty()) throw RuntimeException("$name not provided.")
    return param
}