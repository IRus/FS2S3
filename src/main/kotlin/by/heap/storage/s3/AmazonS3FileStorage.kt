package by.heap.storage.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.CannedAccessControlList
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.S3ObjectSummary
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.FileVisitResult
import java.nio.file.FileVisitor
import java.nio.file.Files.copy
import java.nio.file.Files.createDirectories
import java.nio.file.Files.delete
import java.nio.file.Files.newInputStream
import java.nio.file.Files.size
import java.nio.file.Files.walkFileTree
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.nio.file.attribute.BasicFileAttributes

typealias LocalFiles = Map<String, Path>

typealias RemoteFiles = Map<String, S3ObjectSummary>

class AmazonS3FileStorage(
    private val s3: AmazonS3
) {
    fun push(bucketName: String, path: String) {
        createOrGetBucket(bucketName)

        val s3Objects = listS3Objects(bucketName)
        val fsObjects = listFsObjects(path)

        val (local, remote) = compare(fsObjects, s3Objects)

        deleteRemote(bucketName, remote)
        upload(bucketName, local)
    }

    fun pull(bucketName: String, path: String) {
        createOrGetBucket(bucketName)

        val s3Objects = listS3Objects(bucketName)
        val fsObjects = listFsObjects(path)

        val (local, remote) = compare(fsObjects, s3Objects)

        deleteLocal(local)
        download(bucketName, path.toPath(), remote)
    }

    fun list(bucketName: String) {
        listS3Objects(bucketName).forEach { key, _ ->
            println(s3.getUrl(bucketName, key))
        }

    }

    private fun deleteLocal(files: LocalFiles) {
        files.forEach { key, file ->
            LOGGER.info("Local file '$key' scheduled to delete.")
            delete(file)
        }
    }

    private fun deleteRemote(bucketName: String, files: RemoteFiles) {
        files.forEach { key, _ ->
            LOGGER.info("Remote file '$key' scheduled to delete.")
            s3.deleteObject(bucketName, key)
        }
    }

    private fun download(bucketName: String, root: Path, files: RemoteFiles) {
        files.forEach { key, _ ->
            LOGGER.info("Downloading file '$key' from s3.")
            val s3Object = s3.getObject(bucketName, key)
            val file = root.resolve(key)
            createDirectories(file.parent)
            copy(s3Object.objectContent, file, StandardCopyOption.REPLACE_EXISTING)
            LOGGER.info("File '$key' downloaded from s3.")
        }
    }

    private fun upload(bucketName: String, files: LocalFiles) {
        files.forEach { key, file ->
            val metadata = ObjectMetadata().apply { ->
                this.contentLength = size(file)
            }

            LOGGER.info("Uploading file '$key' to s3.")

            val request = PutObjectRequest(bucketName, key, newInputStream(file), metadata)
                .withCannedAcl(CannedAccessControlList.PublicRead)
            s3.putObject(request)

            LOGGER.info("File '$key' uploaded to s3.")
        }
    }

    private fun compare(fsObjects: LocalFiles, s3Objects: RemoteFiles): Pair<LocalFiles, RemoteFiles> {
        val local = fsObjects.filter { (key, _) -> !s3Objects.containsKey(key) }
        val remote = s3Objects.filter { (key, _) -> !fsObjects.containsKey(key) }

        return local to remote
    }

    private fun Path.toKey(root: Path): String {
        return this.toAbsolutePath().normalize().toString().removePrefix("${root.toString()}/")
    }

    private fun String.toPath(): Path {
        return Paths.get(this).toAbsolutePath().normalize()
    }

    private fun createOrGetBucket(name: String) {
        val bucketExist = s3.doesBucketExist(name)

        if (!bucketExist) {
            LOGGER.info("Bucket $name doesn't exists.")
            s3.createBucket(name)
            LOGGER.info("Bucket $name created.")
        }

        if (bucketExist) {
            LOGGER.info("Bucket $name exists.")
        }
    }

    private fun listFsObjects(path: String): LocalFiles {
        val all = mutableMapOf<String, Path>()
        val root = path.toPath()

        walkFileTree(root, object : FileVisitor<Path> {
            override fun visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult {
                all.put(file.toKey(root), file)
                return FileVisitResult.CONTINUE
            }

            override fun visitFileFailed(file: Path, exc: IOException): FileVisitResult {
                LOGGER.error("Error visiting path '$file'. Terninate.", exc)
                return FileVisitResult.TERMINATE
            }

            override fun postVisitDirectory(dir: Path, exc: IOException?): FileVisitResult {
                return FileVisitResult.CONTINUE
            }

            override fun preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult {
                return FileVisitResult.CONTINUE
            }
        })

        return all
    }

    private fun listS3Objects(name: String): RemoteFiles {
        val all = mutableMapOf<String, S3ObjectSummary>()

        var objects = s3.listObjects(name)
        all.putAll(objects.objectSummaries.map { it.key to it }.toMap())
        do {
            objects = s3.listNextBatchOfObjects(objects)
            all.putAll(objects.objectSummaries.map { it.key to it }.toMap())
        } while (objects.isTruncated)

        return all
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(AmazonS3FileStorage::class.java)
    }
}
