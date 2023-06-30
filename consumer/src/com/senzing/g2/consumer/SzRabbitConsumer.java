package com.senzing.g2.consumer;

// Imports
import com.senzing.g2.engine.G2JNI;
import com.senzing.g2.engine.Result;

import java.io.StringReader;

import java.util.concurrent.*;
import java.util.Set;
import java.util.Iterator;
import java.util.IdentityHashMap;
import java.util.HashSet;

import org.json.JSONObject;  
import org.json.JSONArray;

public class SzSimpleRedoer {

    public static void main(String[] args){
    	int INTERVAL = 1000;
        
        String longRecord = System.getenv("LONG_RECORD");
        long LONG_RECORD = (longRecord!=null) ? Long.parseLong(longRecord)*1000l: 300l*1000l;
        String logLevel = System.getenv("SENZING_LOG_LEVEL");
        String SENZING_LOG_LEVEL = (logLevel!=null) ? logLevel: "info";

        //Setup info and logging
        String engineConfig = System.getenv("SENZING_ENGINE_CONFIGURATION_JSON");

        if(engineConfig == null){
            System.out.println("The environment variable SENZING_ENGINE_CONFIGURATION_JSON must be set with a proper JSON configuration.");
            System.out.println("Please see https://senzing.zendesk.com/hc/en-us/articles/360038774134-G2Module-Configuration-and-the-Senzing-API");
            System.exit(-1);
        }
        int returnCode = 0;
        G2JNI g2 = new G2JNI();
        g2.init("sz_simple_redoer_java", engineConfig, false);
	
        String threads = System.getenv("SENZING_THREADS_PER_PROCESS");
        int maxWorkers = 4;
        if(threads != null){
            maxWorkers = Integer.parseInt(threads);}

        int messages = 0;

        ExecutorService executor = Executors.newFixedThreadPool(maxWorkers);
        System.out.println("Threads: " + maxWorkers);
        int emptyPause = 0;
        
	IdentityHashMap<Future<String>,FutureData> futures=new IdentityHashMap<>();
	CompletionService<String> compService = new ExecutorCompletionService<String>(executor);
	Future<String> doneFuture = null;
	long logCheckTime = System.currentTimeMillis();
	long prevTime = logCheckTime;
    
    
    }

    private static String processMsg(G2JNI engine, String msg, boolean withInfo){
        int returnCode = 0;
	JsonObject record = JsonUtil.parseJsonObject(msg);
        if(withInfo){
            StringBuffer response = new StringBuffer();
            returnCode = engine.addRecordWithInfo(record.getJsonObject("DATA_SOURCE"), record.getJsonObject("RECORD_ID"),
            					  msg, response);
            if(returnCode!=0){
                System.out.println("Exception " + engine.getLastException() + " on message: " + msg);
                return null;
            }
            return response.toString();
        }
        else{
            returnCode = engine.addRecord(record.getJsonObject("DATA_SOURCE"), record.getJsonObject("RECORD_ID"), msg);
            if(returnCode!=0)
                System.out.println("Exception " + engine.getLastException() + " on message: " + msg);
            return null;
        }
    }
}
