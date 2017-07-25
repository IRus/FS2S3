package by.heap.storage.s3

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.slf4j.LoggerFactory

/**
 * Entry point of application.
 *
 * @author Ibragimov Ruslan
 * @since 0.1
 */
object Application {
    @JvmStatic
    fun main(args: Array<String>) {
        val command = args.parse(0, "Command")
        val path = args.parse(1, "Path")

        LOGGER.info("Start application with command '$command' and path '$path'.")

        val storage = AmazonS3FileStorage(AmazonS3ClientBuilder.defaultClient())

        when (command) {
            "pull" -> {
                LOGGER.info("Start pulling local from s3.")
                storage.pull(Config.awsBucketName, path)
            }
            "push" -> {
                LOGGER.info("Start pushing local files to s3.")
                storage.push(Config.awsBucketName, path)
            }
            "list" -> {
                LOGGER.info("List s3 files.")
                storage.list(Config.awsBucketName)
            }
        }
    }

    private val LOGGER = LoggerFactory.getLogger(Application::class.java)
}
