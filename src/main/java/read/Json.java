package main.java.read;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import main.java.documents.Doc;
import main.java.documents.Order;

public class Json {
	
	private ArrayList<Order> orders = new ArrayList<Order>();
	private ArrayList<Doc> orderlines = new ArrayList<Doc>();
	
	public ArrayList<Order> getOrders() {
		return orders;
	}
	public void setOrders(ArrayList<Order> orders) {
		this.orders = orders;
	}
	public ArrayList<Doc> getOrderlines() {
		return orderlines;
	}
	public void setDocs(ArrayList<Doc> docs) {
		this.orderlines = docs;
	}
	
	void parseOrderlineFromOrder(JSONArray jsons) throws java.text.ParseException {
		for (int i = 0; i < jsons.size(); i++) {
			JSONObject obj = (JSONObject)jsons.get(i);
			Doc doc = new Doc();
			doc.setProductId((String)obj.get("OrderId"));
			doc.setAsin((String)obj.get("asin"));
			doc.setTitle((String)obj.get("Title"));
			doc.setPrice((double)obj.get("price"));
			doc.setBrand((String)obj.get("brand"));
			orderlines.add(doc);
		}
	}
	
	Order parseOrderFromJson(JSONObject obj) throws java.text.ParseException {
		SimpleDateFormat formatOrder = new SimpleDateFormat("yyyy-MM-dd");
		Order ord = new Order();
		ord.setOrderId((String)obj.get("OrderId"));
		ord.setPersonId((String)obj.get("PersonId"));
		ord.setOrderDate((Date)formatOrder.parse((String) obj.get("OrderDate")));
		ord.setTotalPrice((Double)obj.get("TotalPrice"));
		parseOrderlineFromOrder((JSONArray)obj.get("Orderline"));
		ord.setOrderline(getOrderlines());
		return ord;
	}

	public void readOrder(String pathToJson) throws java.text.ParseException {
		
		JSONObject obj;
		// This will reference one line at a time
		String line = null;
		
		try {
						
			// FileReader reads text files in the default encoding.
			FileReader fileReader = new FileReader(pathToJson);

			// Always wrap FileReader in BufferedReader.
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			while ((line = bufferedReader.readLine()) != null) {
				obj = (JSONObject) new JSONParser().parse(line);
				orders.add(parseOrderFromJson(obj));
			}
			// Always close files.
			bufferedReader.close();
		} catch (FileNotFoundException ex) {
			System.out.println("Unable to open file '" + file.getAbsolutePath() + "'");
		} catch (IOException ex) {
			System.out.println("Error reading file '" + file.getAbsolutePath() + "'");
			// Or we could just do this:
			// ex.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


}