package com.sparkonlambda.lambda;

import java.io.Serializable;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sparkonlambda.etls.GetObjCountETL;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * LambdaJobInitiator class is a lambda request handler, to demo the understand about invoking spark etl job
 * written in scala or java.
 * Input type can be extended with aws service events (i.e S3Event)
 *
 * Note, even-though spark code written in scala, it should provide input and output based on java objects.
 *
 */
public class LambdaJobInitiator implements RequestHandler<String, Map<String, Object>>, Serializable {

    @Override
    public Map<String, Object> handleRequest(String event, Context context) {


        Map<String, Object> outputMeta = new LinkedHashMap<>();
        LinkedHashMap<String, Object> eventMap = null;
        try {
            eventMap = new ObjectMapper().readValue(event, LinkedHashMap.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        String jtype = String.valueOf(eventMap.get("type"));
        String input_path = String.valueOf(eventMap.get("input_path"));
        String output_dir = String.valueOf(eventMap.get("output_dir"));

        if("GetObjCountETL".equalsIgnoreCase(jtype)){
            outputMeta = new GetObjCountETL().func(eventMap);
        }
        System.out.println("outputMeta "+outputMeta);
        return outputMeta;
    }

}
