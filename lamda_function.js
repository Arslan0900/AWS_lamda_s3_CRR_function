// Import the AWS SDK module
import { S3Client, CopyObjectCommand, ListObjectsV2Command } from "@aws-sdk/client-s3";

// Configure S3 clients for both regions
const sourceS3 = new S3Client({ region: 'eu-central-1' }); // Frankfurt region
const destinationS3 = new S3Client({ region: 'eu-west-2' }); // London region

// Define source and destination buckets
const SOURCE_BUCKET = 'verified-prod-eu-central1';
const DESTINATION_BUCKET = 'backup-prod-eu-central1';

export const handler = async (event) => {
  try {
    // Initialize variables for pagination
    let continuationToken = undefined;
    let hasMoreObjects = true;

    while (hasMoreObjects) {
      // List objects in the source bucket
      const listObjectsCommand = new ListObjectsV2Command({
        Bucket: SOURCE_BUCKET,
        ContinuationToken: continuationToken
      });
      const listObjectsResponse = await sourceS3.send(listObjectsCommand);

      // Check if there are any objects to copy
      if (!listObjectsResponse.Contents || listObjectsResponse.Contents.length === 0) {
        console.log('No objects found in the source bucket.');
        return {
          statusCode: 200,
          body: JSON.stringify('No objects to sync.')
        };
      }

      // Copy objects in parallel
      const copyPromises = listObjectsResponse.Contents.map(async (object) => {
        const objectKey = object.Key;
        const encodedObjectKey = encodeURIComponent(objectKey); // Encode the object key
        const copySource = `${SOURCE_BUCKET}/${encodedObjectKey}`; // Use the encoded key

        try {
          // Copy each object to the destination bucket
          const copyCommand = new CopyObjectCommand({
            CopySource: copySource,
            Bucket: DESTINATION_BUCKET,
            Key: objectKey
          });
          await destinationS3.send(copyCommand);

          console.log(`Successfully copied ${objectKey} to ${DESTINATION_BUCKET}`);
        } catch (copyError) {
          console.error(`Failed to copy ${objectKey}:`, copyError);
        }
      });

      // Wait for all copy operations to complete
      await Promise.all(copyPromises);

      // Check if there are more objects to list
      continuationToken = listObjectsResponse.NextContinuationToken;
      hasMoreObjects = !!continuationToken;
    }

    return {
      statusCode: 200,
      body: JSON.stringify('Sync completed successfully.')
    };

  } catch (error) {
    console.error('Error listing or copying objects:', error);
    return {
      statusCode: 500,
      body: JSON.stringify('Error occurred during sync.')
    };
  }
};
