package com.solbeer.demo.billing;


import java.io.IOException;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jms.annotation.JmsListener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.solbeer.demo.datamodel.*;

@SpringBootApplication
public class SolBeerBillingService {

	public static void main(String[] args) {
		SpringApplication.run(SolBeerBillingService.class, args);
	}
	
	//Listen for Confirmed Orders
	@JmsListener(destination = "ConfirmedOrders_Queue")
	public void onMessage(Message msg) {
		System.out.println("Message Received......");
		ObjectMapper mapper = new ObjectMapper();
		TextMessage txtmsg = (TextMessage) msg;
		OrderInfo confirmedOrder;
		
		try {
			confirmedOrder = mapper.readValue(txtmsg.getText(), OrderInfo.class);
			int orderId = confirmedOrder.getOrderId();
			int orderQty = confirmedOrder.getOrderQty();
			BillingInfo orderBill = null;
			
			orderBill.setOrderId(orderId);
			orderBill.setName(confirmedOrder.getName());
			orderBill.setBillingAddress(confirmedOrder.getShippingAddress());
			orderBill.setOrderedQty(confirmedOrder.getOrderQty());
			orderBill.setItemCost(confirmedOrder.getItemCost());
			orderBill.setTotalCost(orderBill.getItemCost()*orderBill.getOrderedQty());


			
		} catch (IOException | JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


		
	}

}
