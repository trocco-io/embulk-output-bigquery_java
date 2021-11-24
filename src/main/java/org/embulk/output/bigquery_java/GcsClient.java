package org.embulk.output.bigquery_java;

import org.embulk.output.bigquery_java.config.PluginTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcsClient {
    private final Logger logger = LoggerFactory.getLogger(GcsClient.class);
    private String project;
    private String destinationProject;
    private String bucket;
    private String location;


    GcsClient(PluginTask task){
        this.project = task.getProject().get();
        this.destinationProject = task.getDestinationProject().get();
        this.bucket = task.getGcsBucket().get();
        this.location = task.getLocation().get();



    }

    // create bucket
    //        def insert_temporary_bucket(bucket = nil)
    //          bucket ||= @bucket
    //          begin
    //            Embulk.logger.info { "embulk-output-bigquery: Insert bucket... #{@destination_project}:#{bucket}" }
    //            body = {
    //              name: bucket,
    //              lifecycle: {
    //                rule: [
    //                  {
    //                    action: {
    //                      type: "Delete",
    //                    },
    //                    condition: {
    //                      age: 1,
    //                    }
    //                  },
    //                ]
    //              }
    //            }
    //
    //            if @location
    //              body[:location] = @location
    //            end
    //
    //            opts = {}
    //
    //            Embulk.logger.debug { "embulk-output-bigquery: insert_temporary_bucket(#{@project}, #{body}, #{opts})" }
    //            with_network_retry { client.insert_bucket(@project, body, opts) }
    //          rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
    //            if e.status_code == 409 && /conflict:/ =~ e.message
    //              # ignore 'Already Exists' error
    //              return nil
    //            end
    //            response = {status_code: e.status_code, message: e.message, error_class: e.class}
    //            Embulk.logger.error {
    //              "embulk-output-bigquery: insert_temporary_bucket(#{@project}, #{body}, #{opts}), response:#{response}"
    //            }
    //            raise Error, "failed to insert bucket #{@destination_project}:#{bucket}, response:#{response}"
    //          end
    //        end
    //

    /// create bucket
    public void insertTemporaryBucket(String bucket){
        if (bucket == null || bucket.isEmpty()){
            bucket = this.bucket;
        }

        logger.info(String.format("embulk-output-bigquery: Insert bucket... %s:%s", destinationProject, bucket));
        String bucketName = "my_unique_bucket"; // Change this to something unique
        //Bucket bucket = storage.create(BucketInfo.of(bucketName));


    }

    //         def insert_object(path, object: nil, bucket: nil)
    //          bucket ||= @bucket
    //          object ||= path
    //          object = object.start_with?('/') ? object[1..-1] : object
    //          object_uri = URI.join("gs://#{bucket}", object).to_s
    //
    //          started = Time.now
    //          begin
    //            Embulk.logger.info { "embulk-output-bigquery: Insert object... #{path} => #{@destination_project}:#{object_uri}" }
    //            body = {
    //              name: object,
    //            }
    //            opts = {
    //              upload_source: path,
    //              content_type: 'application/octet-stream'
    //            }
    //
    //            Embulk.logger.debug { "embulk-output-bigquery: insert_object(#{bucket}, #{body}, #{opts})" }
    //            # memo: gcs is strongly consistent for insert (read-after-write). ref: https://cloud.google.com/storage/docs/consistency
    //            with_network_retry { client.insert_object(bucket, body, opts) }
    //          rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
    //            response = {status_code: e.status_code, message: e.message, error_class: e.class}
    //            Embulk.logger.error {
    //              "embulk-output-bigquery: insert_object(#{bucket}, #{body}, #{opts}), response:#{response}"
    //            }
    //            raise Error, "failed to insert object #{@destination_project}:#{object_uri}, response:#{response}"
    //          end
    //
    //
    public void insertObject(){

    }

    //         def insert_objects(paths, objects: nil, bucket: nil)
    //          return [] if paths.empty?
    //          bucket ||= @bucket
    //          objects ||= paths
    //          raise "number of paths and objects are different" if paths.size != objects.size
    //
    //          responses = []
    //          paths.each_with_index do |path, idx|
    //            object = objects[idx]
    //            responses << insert_object(path, object: object, bucket: bucket)
    //          end
    //          responses
    //        end
    public void insertObjects(){

    }


    //        def delete_object(object, bucket: nil)
    //          bucket ||= @bucket
    //          object = object.start_with?('/') ? object[1..-1] : object
    //          object_uri = URI.join("gs://#{bucket}", object).to_s
    //          begin
    //            Embulk.logger.info { "embulk-output-bigquery: Delete object... #{@destination_project}:#{object_uri}" }
    //            opts = {}
    //
    //            Embulk.logger.debug { "embulk-output-bigquery: delete_object(#{bucket}, #{object}, #{opts})" }
    //            response = with_network_retry { client.delete_object(bucket, object, opts) }
    //          rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
    //            if e.status_code == 404 # ignore 'notFound' error
    //              return nil
    //            end
    //            response = {status_code: e.status_code, message: e.message, error_class: e.class}
    //            Embulk.logger.error {
    //              "embulk-output-bigquery: delete_object(#{bucket}, #{object}, #{opts}), response:#{response}"
    //            }
    //            raise Error, "failed to delete object #{@destination_project}:#{object_uri}, response:#{response}"
    //          end
    //        end
    public void deleteObject(){


    }



}
