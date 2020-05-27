package main.java.read;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import main.java.documents.Feedback;
import main.java.documents.Person;
import main.java.documents.Product;
import main.java.documents.Vendor;

import java.text.ParseException;
import java.util.ArrayList;

public class Csv {
	
	private ArrayList<Person> persons = new ArrayList<Person>();
	private ArrayList<Feedback> feedbacks = new ArrayList<Feedback>();
	private ArrayList<Product> products = new ArrayList<Product>();
	private ArrayList<Vendor> vendors = new ArrayList<Vendor>();

	public ArrayList<Vendor> getVendors() {
		return vendors;
	}

	public void setVendors(ArrayList<Vendor> vendors) {
		this.vendors = vendors;
	}

	public ArrayList<Product> getProducts() {
		return products;
	}

	public void setProducts(ArrayList<Product> products) {
		this.products = products;
	}

	public ArrayList<Person> getPersons() {
		return persons;
	}

	public void setPersons(ArrayList<Person> persons) {
		this.persons = persons;
	}
	
	public ArrayList<Feedback> getFeedbacks() {
		return feedbacks;
	}

	public void setFeedbacks(ArrayList<Feedback> feedbacks) {
		this.feedbacks = feedbacks;
	}
	
	Product setBrandByProductAsin(String asin,String brand) {
	    for(Integer i = 0; i < products.size(); i++) {
	        if(products.get(i).getAsin().equals(asin)) {
	        	products.get(i).setBrand(brand);
	        }
	    }
	    return null;
	}
	
	Person parseStringToPersonList(String[] data) throws ParseException{
		Person pers = new Person();
		pers.setId(data[0]);
		pers.setFirstName(data[1]);
		pers.setLastName(data[2]);
		pers.setGender(data[3]);
		pers.setBirthday(data[4]);
		pers.setCreateDate(data[5]);
		pers.setLocation(data[6]);
		pers.setBrowserUsed(data[7]);
	    pers.setPlace(data[8]);
	    return pers; 
	}
	
	Feedback parseStringToFeedbackList(String[] data) throws ParseException{
		Feedback feed = new Feedback();
		String[] asinAndId = data[0].split("\\|");
		String asinAndId0 = asinAndId[0] != null ? asinAndId[0]: "null";
		String asinAndId1 = asinAndId[1] != null ? asinAndId[1]: "null";
		String data1 = data[1] != null ? data[1]: "null";
		feed.setAsin(asinAndId0);
		feed.setPersonId(asinAndId1);
		feed.setFeedback(data1);
	    return feed; 
	}
	
	Product parseStringToProductList(String[] data) throws ParseException{
		Product prod = new Product();

		String data0 = data[0] != null ? data[0]: "null";
		String data1 = data[1] != null ? data[1]: "null";
		String data2 = data[2] != null ? data[2]: "null";
		String data3 = data[3] != null ? data[3]: "null";
		prod.setAsin(data0);
		prod.setTitle(data1);
		prod.setPrice(data2);
		prod.setImgUrl(data3);

	    return prod; 
	}
	
	Vendor parseStringToVendorList(String[] data) throws ParseException{
		Vendor vend = new Vendor();
		String data0 = data[0] != null ? data[0]: "null";
		String data1 = data[1] != null ? data[1]: "null";
		String data2 = data[2] != null ? data[2]: "null";
		vend.setVendor(data0);
		vend.setCountry(data1);
		vend.setIndustry(data2);
	    return vend; 
	}
	
	public void readPerson(String pathToCsv) throws IOException, ParseException {
		BufferedReader csvReader = new BufferedReader(new FileReader(pathToCsv));
		csvReader.readLine();// Skip the first line
		String row = null;
		while ((row = csvReader.readLine()) != null) {
			String[] data = row.split("\\|");
			persons.add(parseStringToPersonList(data));
		}
		csvReader.close();
	}
	
	public void readFeedback(String pathToCsv) throws IOException, ParseException {
		BufferedReader csvReader = new BufferedReader(new FileReader(pathToCsv));
		String row = null;
		while ((row = csvReader.readLine()) != null) {
			String data[] = row.split(",");
			feedbacks.add(parseStringToFeedbackList(data));
		}
		csvReader.close();
	}
	
	public void readProduct(String pathToCsv,String pathToCsvByBrand) throws IOException, ParseException {
		BufferedReader csvReader = new BufferedReader(new FileReader(pathToCsv));
		csvReader.readLine();// Skip the first line
		String row = null;

		while ((row = csvReader.readLine()) != null) {
			String data[] = row.split(",");

			products.add(parseStringToProductList(data));
		}
		csvReader.close();

		BufferedReader csvReaderByBrand = new BufferedReader(new FileReader(pathToCsvByBrand));
		while ((row = csvReaderByBrand.readLine()) != null) {
			String data[] = row.split(",");			
			setBrandByProductAsin(data[1],data[0]);
		}
		csvReaderByBrand.close();

	}
	
	public void readVendor(String pathToCsv) throws IOException, ParseException {
		BufferedReader csvReader = new BufferedReader(new FileReader(pathToCsv));
		csvReader.readLine();// Skip the first line
		String row = null;
		while ((row = csvReader.readLine()) != null) {
			String data[] = row.split(",");
			vendors.add(parseStringToVendorList(data));
		}
		csvReader.close();


	}



	
}
