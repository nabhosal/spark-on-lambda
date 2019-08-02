# spark-on-lambda
It's boilerplate code to help build aws lambda based spark jobs, job can be written using java or scala.
We are using the boilerplate code for building etl job for moving hourly & daily data to a datalake.
Before lambda, we used to build ETL in AWS Glue, but due to varying nature of few data-source. the Glue jobs are getting underutilized in the range 60% to 95% with just 2 DPU.
We wanted to move away few jobs which are underutilized on aws glue to aws lambda. 
If a job on lambda is not able to handle data workloads then we can easily migrate the job to glue since the same spark code can be deployed at both service with minimal changes.

To build zip to deploy on lambda

``./gradlew buildZip
``

Upload zip through S3, since zip size is 187MB

Set below environment variables

```
 SPARK_LOCAL_IP = 127.0.0.1
 S3N_AWS_ACCESSKEY = <AWS_ACCESSKEY>
 S3N_AWS_SECRETKEY = <AWS_SECRETKEY>
```

Set handler
``com.sparkonlambda.lambda.LambdaJobInitiator::handleRequest``

Performance

| TestCase | data size | no of objects | time | observation |
| ------------- |:-------------:| -----:| ----:| ----:|
|Big file| 1.5GB |1 | 40 sec| Very good performance, it can work till 2 GB based on transformation complexities 
|Small files(in kb) in large number|365.8 MB | 51000|900 sec| Failed due to Timeout, S3 object / file listing operation is very costly on hadoop |
|Small files(in kb) in reasonable number|33.7 MB | 5891|245 sec|  Very good performance, by interpolating the stats we can say it can support reading till 15k records |

Todo
+ Generating temporary credential instead of using access_key * secret_key or better using IAM role of lambda 