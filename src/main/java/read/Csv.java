package main.java.read;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;

import main.java.documents.Feedback;
import main.java.documents.Person;
import main.java.documents.Product;
import main.java.documents.Vendor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalAccessor;
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
	
	public static boolean isNumeric(String strNum) {
	    if (strNum == null) {
	        return false;
	    }
	    try {
	        double d = Double.parseDouble(strNum);
	    } catch (NumberFormatException nfe) {
	        return false;
	    }
	    return true;
	}

	Date convertIsoStringToDate(String s) {
		DateTimeFormatter formatter = new DateTimeFormatterBuilder()
				// date/time
				.append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
				// offset (hh:mm - "+00:00" when it's zero)
				.optionalStart().appendOffset("+HH:MM", "+00:00").optionalEnd()
				// offset (hhmm - "+0000" when it's zero)
				.optionalStart().appendOffset("+HHMM", "+0000").optionalEnd()
				// offset (hh - "Z" when it's zero)
				.optionalStart().appendOffset("+HH", "Z").optionalEnd()
				// create formatter
				.toFormatter();
		String str = OffsetDateTime.parse(s, formatter).toString();
		TemporalAccessor ta = DateTimeFormatter.ISO_INSTANT.parse(str);
		Instant i = Instant.from(ta);
		Date d = Date.from(i);
		return d;
	}
	
	Person parseStringToPersonList(String[] data) throws ParseException{
		SimpleDateFormat formatBirthday = new SimpleDateFormat("yyyy-MM-dd");
		Person pers = new Person();
		pers.setId(data[0]);
		pers.setFirstName(data[1]);
		pers.setLastName(data[2]);
		pers.setGender(data[3]);
		pers.setBirthday((Date)formatBirthday.parse(data[4]));
		pers.setCreateDate(convertIsoStringToDate(data[5]));
		pers.setLocation(data[6]);
		pers.setBrowserUsed(data[7]);
	    pers.setPlace(Integer.parseInt(data[8]));
	    return pers; 
	}
	
	Feedback parseStringToFeedbackList(String[] data) throws ParseException{
		Feedback feed = new Feedback();
		String[] asinAndId = data[0].split("\\|");
		feed.setAsin(asinAndId[0]);
		feed.setPersonId(asinAndId[1]);
		feed.setFeedback(data[1]);
	    return feed; 
	}
	
	Product parseStringToProductList(String[] data) throws ParseException{
		Product prod = new Product();
		prod.setAsin(data[0]);
		prod.setTitle(data[1]);
		if(isNumeric(data[2]))
			prod.setPrice(Float.parseFloat(data[2]));
		prod.setImgUrl(data[3]);
	    return prod; 
	}
	
	Vendor parseStringToVendorList(String[] data) throws ParseException{
		Vendor vend = new Vendor();
		vend.setVendor(data[0]);
		vend.setCountry(data[1]);
		vend.setIndustry(data[2]);
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
