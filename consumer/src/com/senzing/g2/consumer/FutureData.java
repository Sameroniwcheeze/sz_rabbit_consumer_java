package com.senzing.g2.consumer;

public class FutureData {

   long time;
   String message;
   boolean ackd;
   long deliveryTag;
   
   public FutureData(String msg){
   	message = msg;
   }
}
