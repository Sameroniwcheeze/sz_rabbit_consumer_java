package com.senzing.g2.consumer;

// Imports
import com.senzing.g2.engine.G2JNI;
import com.senzing.g2.engine.Result;

import com.rabbitmq.client.*;

import java.io.StringReader;

import java.util.concurrent.*;
import java.util.Set;
import java.util.Iterator;
import java.util.IdentityHashMap;
import java.util.HashSet;

import java.nio.charset.StandardCharsets;

import javax.json.*;

public class SzRabbitConsumer {

    public static void main(String[] args){
    	int INTERVAL = 1000;
        
        String longRecord = System.getenv("LONG_RECORD");
        long LONG_RECORD = (longRecord!=null) ? Long.parseLong(longRecord)*1000l: 300l*1000l;
        String logLevel = System.getenv("SENZING_LOG_LEVEL");
        String SENZING_LOG_LEVEL = (logLevel!=null) ? logLevel: "info";

        //Setup info and logging
        String engineConfig = System.getenv("SENZING_ENGINE_CONFIGURATION_JSON");
	String queue = System.getenv("SENZING_RABBITMQ_QUEUE");
	String amqpUrl = System.getenv("SENZING_AMQP_URL");
	if(queue == null || amqpUrl == null){
		System.out.println("The environment variables SENZING_RABBITMQ_QUEUE and SENZING_AMQP_URL need to be set");
		System.exit(-1);
	}
        if(engineConfig == null){
            System.out.println("The environment variable SENZING_ENGINE_CONFIGURATION_JSON must be set with a proper JSON configuration.");
            System.out.println("Please see https://senzing.zendesk.com/hc/en-us/articles/360038774134-G2Module-Configuration-and-the-Senzing-API");
            System.exit(-1);
        }
        int returnCode = 0;
        G2JNI g2 = new G2JNI();
        g2.init("sz_rabbit_consumer_java", engineConfig, false);
	
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
	
	Connection conn = null;
	Channel ch = null;
        try{
        	ConnectionFactory factory = new ConnectionFactory();
		factory.setUri(amqpUrl);
		conn = factory.newConnection();
		ch = conn.createChannel();
		ch.queueDeclare(queue, false, false, false, null);
		ch.basicQos(maxWorkers);
				
		String cTag = "Senzing-rabbit-consumer";
		while(true){
			if(!futures.isEmpty()){
				doneFuture = compService.poll(10, TimeUnit.SECONDS);
				while(doneFuture!=null){
					messages++;
					if(!(futures.get(doneFuture).ackd)){
						ch.basicAck(futures.get(doneFuture).deliveryTag, false);
						futures.get(doneFuture).ackd = true;
					}
					futures.remove(doneFuture);
					doneFuture = compService.poll();
					if(messages%INTERVAL==0 && messages != 0){
						double diff = (System.currentTimeMillis()-prevTime)/1000.0;
						double speed = (diff>0.0) ? ((double)INTERVAL/diff): 0;
						System.out.printf("Added " + String.valueOf(messages) + " records, %.0f records per second\n", speed);
						prevTime = System.currentTimeMillis();
					}
				}
			}
			
			if(System.currentTimeMillis()>(logCheckTime+(LONG_RECORD/2))){
				int numStuck = 0;
				System.out.println(g2.stats());
				for (Future<String> future: futures.keySet()) {
					FutureData longRecordData= futures.get(future);
					long time = longRecordData.time;
					if(LONG_RECORD <= System.currentTimeMillis() - time){
						String longRecordMsg = longRecordData.message;
						System.out.printf("This record has been processing for %.2f minutes\n", (System.currentTimeMillis()-time)/(1000.0*60.0));
						if(2*LONG_RECORD <= System.currentTimeMillis() - time){
							ch.basicReject(futures.get(future).deliveryTag, false);
							futures.get(doneFuture).ackd = true;
						}
						System.out.println(longRecordMsg);
						numStuck++;
					}
					if(numStuck>=maxWorkers){
						System.out.println("All " + String.valueOf(maxWorkers) + " threads are stuck on long records");
					}
				}
				logCheckTime=System.currentTimeMillis();
			}
		        //Add processing the messages to the queue until the amount in the queue is equal to the number of workers.
		        while(futures.size()<maxWorkers){
		        	GetResponse delivery = ch.basicGet(queue, false);
		        	if(delivery == null){
		        		TimeUnit.MICROSECONDS.sleep(500);

		        	}
		        	else{
					String msg = new String(delivery.getBody(), StandardCharsets.UTF_8);
					FutureData futDat = new FutureData(msg);
					futDat.deliveryTag = delivery.getEnvelope().getDeliveryTag();
			    	        Future<String> putFuture = compService.submit(() -> processMsg(g2, msg, true, futDat));
			    	        futures.put(putFuture, futDat);
		        	}
			}
		}
	}
	catch(Exception e){
		e.printStackTrace(System.out);
	    System.out.println("Added a total of " + String.valueOf(messages) + " records");
	    try{
	    	if(ch!=null){
	    		ch.close();}
	    }
	    catch(Exception ex){
	    System.out.println(ex);}
	    
            executor.shutdown();
            g2.destroy();
	    System.exit(0);
	}
    }

    private static String processMsg(G2JNI engine, String msg, boolean withInfo, FutureData futDat){
        int returnCode = 0;
	JsonObject record = JsonUtil.parseJsonObject(msg);
	futDat.time = System.currentTimeMillis();
        if(withInfo){
            StringBuffer response = new StringBuffer();
            returnCode = engine.addRecordWithInfo(record.getString("DATA_SOURCE"), record.getString("RECORD_ID"),
            					  msg, null, engine.G2_RECORD_DEFAULT_FLAGS, response);
            if(returnCode!=0){
                System.out.println("Exception " + engine.getLastException() + " on message: " + msg);
                return null;
            }
            return response.toString();
        }
        else{
            returnCode = engine.addRecord(record.getString("DATA_SOURCE"), record.getString("RECORD_ID"), msg, null);
            if(returnCode!=0)
                System.out.println("Exception " + engine.getLastException() + " on message: " + msg);
            return null;
        }
    }
}
