package com.asirtech.kafkaconsumer;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asirtech.kafkaconsumer.DB.DbConnection;
import com.asirtech.kafkaconsumer.DTO.StudentModel;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Hello world!
 *
 */
public class ReadDataConsumer 
{
    public static void main( String[] args ) throws SQLException
    {
    	
    	 Logger logger= LoggerFactory.getLogger(ReadDataConsumer.class.getName());  
         String bootstrapServers="localhost:9092";  
         String grp_id="third_app";
         String topic="student-details";  
         //Creating consumer properties  
         Properties properties=new Properties();  
         properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);  
         properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class);  
         properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);  
         properties.put(ConsumerConfig.GROUP_ID_CONFIG,grp_id);  
         properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");  
         
         Connection con = DbConnection.connectDb();
         
         try (KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(properties)) {
        	 consumer.subscribe(Arrays.asList(topic));  
//			 System.out.println( "Hello World!" );
        	 ObjectMapper objectMapper = new ObjectMapper();
        	 DatabaseMetaData dmd;
        	 DataProvider dp = new DataProvider();
        	 while(true){  
				 
			     ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(100));  
			     
			     for(ConsumerRecord<String,String> record: records){  
			    	 String tablename = record.key();
			    	 String val = record.value();
			    	 
			    	 StudentModel value = objectMapper.readValue(val, StudentModel.class);
			    	 
			    	 dmd = con.getMetaData();
			    	 ResultSet tables = dmd.getTables(null,null,tablename,null);
			    	 
			    	 if(tables.next()) {
//			    		 System.out.println("Table Exists");
			    		 dp.dataProvider(tablename,value, con);
			    	 }else {
			    		 String sql = "CREATE TABLE " + tablename + "("
			    				 + "reg_no INT, "
			    				 + "first_name TEXT, "
			    				 + "last_name TEXT, " 
			    				 + "location TEXT, "
			    				 + "state TEXT, "
			    				 + "language TEXT);"; 
			    		 
			    		 Statement statement = con.createStatement();
			    		 int i = statement.executeUpdate(sql);
			    		 
			    		 if(i == 0) {
			    			 dp.dataProvider(tablename,value, con);
			    			 System.out.println("---> Table "+ tablename +" Created Sucessfully.. ");
			    		 }else {
			    			 System.out.println("Table creation failed!..");
			    		 }
			    	 }
			    	 
			    	 System.out.println("-->" + record);
			         logger.info("Consumer Record Value: "+record.value());  
			     }  
			 }
		}catch(Exception e) {
			e.printStackTrace();
		}
       
    }
}
