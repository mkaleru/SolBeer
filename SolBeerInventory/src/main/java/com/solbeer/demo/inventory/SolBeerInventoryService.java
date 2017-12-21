package com.solbeer.demo.inventory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Service;


import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.solbeer.demo.datamodel.OrderInfo;
import com.solbeer.demo.datamodel.Product;


import org.bson.Document;

import static com.mongodb.client.model.Filters.*;



@SpringBootApplication
@Service
public class SolBeerInventoryService{

	private static final Logger log = LoggerFactory.getLogger(SolBeerInventoryService.class);
	private static final String SOL_BEER_INV = "solBeerInventory";
	
	OrderInfo incomingOrder = null;
	Product incomingProduct = null;

	@Autowired
	private JmsTemplate jmsTemplate = null;

	
	public static void main(String[] args) {
		SpringApplication.run(SolBeerInventoryService.class, args);
	}
	
	@PostConstruct
	private void fixJMSTemplate() {
		// Code that makes the JMS Template Cache Connections for Performance.
		CachingConnectionFactory ccf = new CachingConnectionFactory();
		ccf.setTargetConnectionFactory(jmsTemplate.getConnectionFactory());
		jmsTemplate.setConnectionFactory(ccf);
		jmsTemplate.setPubSubDomain(true);
	}

	//Listen for Order
	@JmsListener(destination = "GetOrderData_Queue")
	public void onMessage(Message msg) {
		System.out.println("Message Received......");
		ObjectMapper mapper = new ObjectMapper();
		TextMessage txtmsg = (TextMessage) msg;

		int orderQty = 0;
		
		try {
			incomingOrder = mapper.readValue(txtmsg.getText(), OrderInfo.class);
			incomingProduct = incomingOrder.getProduct();
			orderQty = incomingOrder.getOrderQty();
			
		} catch (JsonParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (JsonMappingException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (JMSException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		log.info("Processing CSC: " + incomingProduct.getCsc());
		getInventory(incomingProduct.getCsc(), orderQty);

				
	}
		
	public void getInventory(int prodCsc, int orderCount) {
		
		log.info("prodCsc: " + prodCsc);
		log.info("Order Qty: " + orderCount);
		
		//Mongo mongo = new Mongo("localhost", 27017);
		//DB db = mongo.getDB("solacedb");
		
		MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
		MongoDatabase solBeerdatabase = mongoClient.getDatabase("solacedb");

		//DBCollection solBeerInvColl = db.getCollection(SOL_BEER_INV);
		
		MongoCollection<Document> solBeerInventoryColl = solBeerdatabase.getCollection(SOL_BEER_INV);
		
		
		
		BasicDBObject andQuery = new BasicDBObject();
		
		List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
		obj.add(new BasicDBObject("product.csc", prodCsc));
		obj.add(new BasicDBObject("storeInventory.productQty", new BasicDBObject("$gt", orderCount)));
		
		//andQuery.put("$and", obj);
		
		//whereQuery.put("product.csc", prodCsc);
		//DBCursor cursor = solBeerInvColl.find(andQuery);

				
		Document inventory = solBeerInventoryColl.find(and(eq("product.csc", prodCsc), gte("storeInventory.productQty", orderCount))).first();
		
		
		if(inventory.size() > 0)
			try {
					//System.out.println(inventory);
					Document product = (Document)inventory.get("product");
					//System.out.println("********************" + product.getDouble("price"));
					incomingOrder.setItemCost(product.getDouble("price"));
					this.jmsTemplate.convertAndSend("ConfirmedOrders_Topic", incomingOrder.toString());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}		 
		
	}
}

