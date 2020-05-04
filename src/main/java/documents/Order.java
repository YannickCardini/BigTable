package main.java.documents;

import java.util.ArrayList;
import java.util.Date;

public class Order {
	
	public String getPersonId() {
		return PersonId;
	}
	public void setPersonId(String personId) {
		PersonId = personId;
	}
	public Date getOrderDate() {
		return OrderDate;
	}
	public void setOrderDate(Date orderDate) {
		OrderDate = orderDate;
	}
	public double getTotalPrice() {
		return TotalPrice;
	}
	public void setTotalPrice(double totalPrice) {
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
	private Date OrderDate;
	private double TotalPrice;
	private ArrayList<Doc> Orderline; 
	

}
