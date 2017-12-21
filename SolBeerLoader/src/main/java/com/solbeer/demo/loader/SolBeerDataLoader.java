package com.solbeer.demo.loader;

import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;


import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;


import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
import com.solbeer.demo.datamodel.Product;
import com.solbeer.demo.datamodel.ProductInventoryData;
import com.solbeer.demo.datamodel.StoreInventory;

@SpringBootApplication
@EnableScheduling
public class SolBeerDataLoader {

	private static final Logger log = LoggerFactory.getLogger(SolBeerDataLoader.class);

	public static void main(String args[]) {
		SpringApplication.run(SolBeerDataLoader.class);
	}
	
    
	@Scheduled(fixedRate = 86400000)     //Run every 24 hours
	public void sendPriceList() throws Exception {

		ObjectMapper mapper = new ObjectMapper();
		log.info("Starting: Request to Utah DABC For Product List");
		try {
			List<Product> dabcProductList = getPriceList();
			log.info("Starting to Publish All Products in Utah DABC List");
			
//			Mongo mongo = new Mongo("localhost", 27017);
//			DB db = mongo.getDB("solacedb");
			
			MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
			MongoDatabase solBeerdatabase = mongoClient.getDatabase("solacedb");
			

//			DBCollection collection = db.getCollection("solBeerColl");
			MongoCollection<org.bson.Document> collection = solBeerdatabase.getCollection("solBeerColl");
			
			
			//DBCollection inventory = db.getCollection("solBeerInventory");
			MongoCollection<org.bson.Document> inventory = solBeerdatabase.getCollection("solBeerInventory");

			
			BasicDBObject document = new BasicDBObject();

			//collection.remove(document);
			collection.drop();
			
			//inventory.remove(document);
			inventory.drop();

			RestTemplate restTemplate = new RestTemplate();
			String inital = restTemplate.getForObject(
					"https://webapps2.abc.utah.gov/Production/OnlineInventoryQuery/IQ/InventoryQuery.aspx", String.class);

			Document doc = Jsoup.parse(inital);
			String VIEWSTATE = doc.select("input[id$=__VIEWSTATE]").get(0).attributes().get("value");
			String EVENTVALIDATION = doc.select("input[id$=__EVENTVALIDATION]").get(0).attributes().get("value");

			int productCount = 0;
			
								
			for (Product p : dabcProductList) {
				String classCode = p.getClass_code();
				if (!classCode.isEmpty()) {
					
					
					//DBObject dbObject = (DBObject) JSON.parse(mapper.writeValueAsString(p));

					//insert "product" into "solBeerColl"
					//collection.insert(dbObject);
					collection.insertOne(org.bson.Document.parse(mapper.writeValueAsString(p)));
					productCount++;
					
					//For the "product" insert the inventory into "solBeerInventory"

	
					//log.info("Processing CSC --- VIEWSTATE: " + VIEWSTATE);
					//log.info("Processing CSC --- EVENTVALIDATION: " + EVENTVALIDATION);
					
					HttpHeaders headers = new HttpHeaders();
					headers.add("User-Agent", "Mozilla");

					MultiValueMap<String, String> map = new LinkedMultiValueMap<String, String>();
					map.add("__VIEWSTATE", VIEWSTATE);
					map.add("__EVENTVALIDATION", EVENTVALIDATION);
					map.add("__ASYNCPOST", "true");
					map.add("ctl00$ContentPlaceHolderBody$tbCscCode", Integer.toString(p.getCsc()));

					HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<MultiValueMap<String, String>>(map, headers);

					ResponseEntity<String> response = restTemplate.postForEntity(
							"https://webapps2.abc.utah.gov/Production/OnlineInventoryQuery/IQ/InventoryQuery.aspx", request,
							String.class);

					Document responseDoc = Jsoup.parse(response.toString());
					ProductInventoryData prodInv = new ProductInventoryData();
					if (responseDoc.select("#ContentPlaceHolderBody_lblWhsInv").size() > 0) {
						prodInv.setWarehouseInventoryQty(
								Integer.parseInt(responseDoc.select("#ContentPlaceHolderBody_lblWhsInv").get(0).text()));
						prodInv.setWarehouseOnOrderQty(
								Integer.parseInt(responseDoc.select("#ContentPlaceHolderBody_lblWhsOnOrder").get(0).text()));
						prodInv.setProductStatus(responseDoc.select("#ContentPlaceHolderBody_lblStatus").get(0).text());
						prodInv.setProduct(p);


						Elements table = responseDoc.select("#ContentPlaceHolderBody_gvInventoryDetails");
						Elements tableRows = table.select("tr[class='gridViewRow'");
						for (Element row : tableRows) {
							StoreInventory storeInv = new StoreInventory();
							Elements data = row.select("td");
							storeInv.setStoreID(data.get(0).text());
							storeInv.setStoreName(data.get(1).text());
							storeInv.setProductQty(Integer.parseInt(data.get(2).select("span").get(0).text()));
							storeInv.setStoreAddress(data.get(3).text());
							storeInv.setStoreCity(data.get(4).text());
							storeInv.setStorePhone(data.get(5).text());
							prodInv.setStoreInventory(storeInv);
							
							log.info("Product inserted :" + mapper.writeValueAsString(prodInv));
							//DBObject prodInventory = (DBObject) JSON.parse(mapper.writeValueAsString(prodInv));
							//inventory.insert(prodInventory);
							inventory.insertOne(org.bson.Document.parse(mapper.writeValueAsString(prodInv)));
						}

					}
					

					log.info("Inserted into mongodb: " + productCount + ":" + mapper.writeValueAsString(p));
					
					
				}
			}
			log.info("Completed Publishing Products");
		} catch (Exception e) {
			log.error("Problem Parsing or Accessing Price list");
			throw e;
		}
	}

	private List<Product> getPriceList()
			throws JsonGenerationException, JsonMappingException, IOException, ParseException {
		List<Product> productList = new ArrayList<Product>();
		RestTemplate restTemplate = new RestTemplate();
		String quote = restTemplate.getForObject(
				"https://webapps2.abc.utah.gov/Production/OnlinePriceList/DisplayPriceList.aspx", String.class);
		Document doc = Jsoup.parse(quote);
		Elements tables = doc.select("table");
		for (Element element : tables) {
			Elements trs = element.select("tr");
			for (Element tr : trs) {
				Product p = new Product();
				Elements tds = tr.select("td");
				if (tds.size() != 0) {
					p.setName(tds.get(0).text());
					p.setDiv_code(tds.get(1).text());
					p.setDept_code(tds.get(2).text());
					p.setClass_code(tds.get(3).text());
					p.setSize(Integer.parseInt(tds.get(4).text()));
					p.setCsc(Integer.parseInt(tds.get(5).text()));
					p.setPrice(NumberFormat.getCurrencyInstance(Locale.US).parse(tds.get(6).text()).doubleValue());
					p.setSPA(tds.get(7).text());
					productList.add(p);
				}
			}
		}
		return productList;
	}
}
