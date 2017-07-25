package by.heap.storage.s3

import java.lang.System.getenv

/**
 * Configuration of application.
 *
 * @author Ibragimov Ruslan
 * @since 0.1
 */
object Config {
    val awsBucketName = getenv("AWS_BUCKET_NAME") ?: "default"
}
