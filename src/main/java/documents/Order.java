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
	public ArrayList<Doc> getOrderline() {
		return Orderline;
	}
	public void setOrderline(ArrayList<Doc> orderline) {
		Orderline = orderline;
	}

	private String OrderId;
	private String PersonId;
	private String OrderDate;
	private String TotalPrice;
	private ArrayList<Doc> Orderline; 
	

}
