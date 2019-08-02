package com.sparkonlambda.lambda.test;

import com.sparkonlambda.lambda.LambdaJobInitiator;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.Map;

public class TestLambdaJobInitiator extends TestCase {

    @Test
    public void testJob(){
        LambdaJobInitiator initiator = new LambdaJobInitiator();

        String input = "{\"type\":\"GetObjCountETL\",\"input_path\":\"s3a://aws-athena-query-results-123456789012-ap-south-1/Unsaved/2019/07/29/fae21f29-fea8-43b4-b8f7-4beb233261fe.csv\",\"output_dir\":\"s3a://destination_bucket/spark-on-lambda/write/largefile/\",\"source_ds_type\":\"csv\"}";

        Map result = initiator.handleRequest(input, null);

        assertEquals("Row count is not same", result.get("total_rows"), 16000);
    }
}
