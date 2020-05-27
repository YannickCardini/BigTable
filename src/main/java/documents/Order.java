package main.java.documents;

import java.util.ArrayList;

public class Order {
	
	public String getPersonId() {
		return PersonId;
	}
	public void setPersonId(String personId) {
		PersonId = personId;
	}
	public String getOrderDate() {
		return OrderDate;
	}
	public void setOrderDate(String orderDate) {
		OrderDate = orderDate;
	}
	public String getTotalPrice() {
		return TotalPrice;
	}
	public void setTotalPrice(String totalPrice) {
		TotalPrice = totalPrice;
	}
	public String getOrderId() {
		return OrderId;
	}
	public void setOrderId(String orderId) {
		OrderId = orderId;
	}
//	public ArrayList<Doc> getOrderline() {
//		return Orderline;
//	}
//	public void setOrderline(ArrayList<Doc> orderline) {
//		Orderline = orderline;
//	}

	public String getOrderLine() {
		return OrderLine;
	}
	public void setOrderLine(String orderLine) {
		OrderLine = orderLine;
	}

	private String OrderId;
	private String PersonId;
	private String OrderDate;
	private String TotalPrice;
	private String OrderLine;
//	private ArrayList<Doc> Orderline; 
	

}
