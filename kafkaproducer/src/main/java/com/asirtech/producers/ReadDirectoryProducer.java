package com.asirtech.producers;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;
import java.util.Properties;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.asirtech.producers.DTO.StudentModel;




/**
 * Hello world!
 *
 */
public class ReadDirectoryProducer {
	private static final Logger logger = LogManager.getLogger();

	public static boolean sendMessage(Path filename) throws IOException {
		Properties props = new Properties();
//        String grp_id="third_app";
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//		props.put(ProducerConfig.CLIENT_ID_CONFIG, grp_id); 
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		
		
		KafkaProducer<String, StudentModel> producer = new KafkaProducer<String, StudentModel>(props);
		logger.info("Producer is created...");
		
		ReadCSV readCSV = new ReadCSV("data/source/"+filename);
		List<?> studentList = readCSV.ReadCSVFile();
		
		System.out.println(studentList);
		for(Object stuObject :studentList ) {
			StudentModel stdRecord= (StudentModel) stuObject;
			String file= filename.toString().replace(".csv", "");
		
			ProducerRecord<String, StudentModel> record = new ProducerRecord<String,StudentModel>("student-details",file,stdRecord);
					
			producer.send(record);
			System.out.println(record);			
		}
		producer.close();
		
		Path temp = Files.move(Paths.get("data/source/"+filename), Paths.get("data/completed/" + filename));
		
		if(temp!=null) {
			System.out.println(filename+ " has been read and send all records to Kafka");
			return true;
		}else {
			System.out.println("Failed..");
			return false;
		}
	}
	
//	@SuppressWarnings({ "null", "unchecked" })
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		try {
			WatchService watcher = FileSystems.getDefault()
					.newWatchService();
			Path dir = Paths.get("data/source/");
			dir.register(watcher, StandardWatchEventKinds.ENTRY_CREATE);
			System.out.println("Watcher Service registered for dir: "+dir.getFileName());
			
			while(true) {
				WatchKey key ;
				try{
					key = watcher.take();
					
				}catch(InterruptedException e) {
					return;
				}
				
				for(WatchEvent<?> event : key.pollEvents()) {
					WatchEvent.Kind<?> kind = event.kind();
					
					//Get the file name
					WatchEvent<Path> ev =(WatchEvent<Path>) event;
					Path filename = ev.context();
					if(kind == StandardWatchEventKinds.ENTRY_CREATE) {
						if(ReadDirectoryProducer.sendMessage(filename)) {
							logger.info("All records from " + filename +" has been sent to Kafka.");
						}else {
							logger.info("Failed..");
						}
					}
				}
				
				boolean value = key.reset();
				if(!value) {
					break;
				}
			}
			
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
		System.out.println("Hello World!");
	}
}
