package com.avis.aws.s3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

import org.xmlpull.v1.XmlPullParserException;

import io.minio.MinioClient;
import io.minio.Result;
import io.minio.errors.InvalidEndpointException;
import io.minio.errors.InvalidPortException;
import io.minio.errors.MinioException;
import io.minio.messages.Bucket;
import io.minio.messages.Item;

public class MinioCrudSample {
	public static void main(String[] args) throws IOException, InvalidEndpointException, InvalidPortException, InvalidKeyException, NoSuchAlgorithmException, XmlPullParserException {
		// Create a minioClient with the Minio Server name, Port, Access key and Secret key.
		MinioClient minioClient = new MinioClient("http://localhost:9000", "admin", "password");

		String bucketName = "my-first-s3-bucket-" + UUID.randomUUID();
		String key = "MyObjectKey" + UUID.randomUUID();

		System.out.println("===========================================");
		System.out.println("Getting Started with Minio");
		System.out.println("===========================================\n");

		try {
			//make bucket
			System.out.println("Creating bucket " + bucketName + "\n");
			minioClient.makeBucket(bucketName);

			//list buckets
			System.out.println("Listing buckets");
			for (Bucket bucket : minioClient.listBuckets()) {
				System.out.println(" - " + bucket.name());
			}
			System.out.println();

			//upload object
			System.out.println("Uploading a new object to minio from a file\n");
			minioClient.putObject(bucketName, key, new FileInputStream(createSampleFile()), key);

			// Download an object
			System.out.println("Downloading an object");
			InputStream is = minioClient.getObject(bucketName, key);
			displayTextInputStream(is);

			// listing objects
			System.out.println("Listing objects");
			Iterable<Result<Item>> myObjects = minioClient.listObjects(bucketName);
	        for (Result<Item> result : myObjects) {
	            Item item = result.get();
	            System.out.println(item.lastModified() + ", " + item.size() + ", " + item.objectName());
	          }
			System.out.println();

			//delete object
			System.out.println("Deleting an object\n");
			minioClient.removeObject(bucketName, key);

			/*
			 * Delete a bucket - A bucket must be completely empty before it can
			 * be deleted, so remember to delete any objects from your buckets
			 * before you try to delete them.
			 */
			System.out.println("Deleting bucket " + bucketName + "\n");
			minioClient.removeBucket(bucketName);
		} catch (MinioException me) {
			System.out.println("Error occurred: " + me);
		}
	}

	/**
	 * Creates a temporary file with text data to demonstrate uploading a file
	 * to Amazon S3
	 *
	 * @return A newly created temporary file with text data.
	 *
	 * @throws IOException
	 */
	private static File createSampleFile() throws IOException {
		File file = File.createTempFile("aws-java-sdk-", ".txt");
		file.deleteOnExit();

		Writer writer = new OutputStreamWriter(new FileOutputStream(file));
		writer.write("abcdefghijklmnopqrstuvwxyz\n");
		writer.write("01234567890112345678901234\n");
		writer.write("!@#$%^&*()-=[]{};':',.<>/?\n");
		writer.write("01234567890112345678901234\n");
		writer.write("abcdefghijklmnopqrstuvwxyz\n");
		writer.close();

		return file;
	}

	/**
	 * Displays the contents of the specified input stream as text.
	 *
	 * @param input
	 *            The input stream to display as text.
	 *
	 * @throws IOException
	 */
	private static void displayTextInputStream(InputStream input) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		while (true) {
			String line = reader.readLine();
			if (line == null)
				break;

			System.out.println("    " + line);
		}
		System.out.println();
	}

}
