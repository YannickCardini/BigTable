package src.main.java.setup;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.buckets.FetchBucketProperties;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.kv.UpdateValue;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.netty.RiakResponseException;
import com.basho.riak.client.core.operations.FetchBucketPropsOperation.Response;
import com.basho.riak.client.core.query.BucketProperties;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;

import src.main.java.documents.Feedback;
import src.main.java.documents.Order;
import src.main.java.documents.Person;
import src.main.java.documents.Product;
import src.main.java.documents.Vendor;
import src.main.java.read.Csv;
import src.main.java.read.Json;
import src.main.java.read.Xml;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.concurrent.ExecutionException;

public class TasteOfRiak {
	// A basic POJO class to demonstrate typed exchanges with Riak
	public static final String PATH = "/home/loubard/Documents/BD_BigTable/DATA/";

	// This will create a client object that we can use to interact with Riak
	private static RiakCluster setUpCluster() throws UnknownHostException {
		// This example will use only one node listening on localhost:10017
		RiakNode node = new RiakNode.Builder().withRemoteAddress("127.0.0.1").withRemotePort(8087).build();

		// This cluster object takes our one node as an argument
		RiakCluster cluster = new RiakCluster.Builder(node).build();

		// The cluster must be started to work, otherwise you will see errors
		cluster.start();

		return cluster;
	}

	private static void storePerson(String pathToCsv, RiakClient client)
			throws IOException, ParseException, ExecutionException, InterruptedException {
		Csv csv = new Csv();
		csv.readPerson(pathToCsv);
		for (Person per : csv.getPersons()) {
			Namespace personsBucket = new Namespace("person");
			Location personLocation = new Location(personsBucket, per.getId());
			StoreValue storePersonOp = new StoreValue.Builder(per).withLocation(personLocation).build();
			client.execute(storePersonOp);
		}
	}

	private static void storeFeedback(String pathToCsv, RiakClient client)
			throws IOException, ParseException, ExecutionException, InterruptedException {
		Csv csv = new Csv();
		csv.readFeedback(pathToCsv);
		for (Feedback feed : csv.getFeedbacks()) {
			Namespace personsBucket = new Namespace("feedback");
			Location personLocation = new Location(personsBucket, feed.getAsin() + feed.getPersonId());// Combo asin +
																										// personId car
																										// aucun des 2
																										// uniques
			StoreValue storePersonOp = new StoreValue.Builder(feed).withLocation(personLocation).build();
			client.execute(storePersonOp);
		}
	}

	private static void storeProduct(String pathToCsv, String pathToCsvByBrand, RiakClient client)
			throws IOException, ParseException, ExecutionException, InterruptedException {
		Csv csv = new Csv();
		csv.readProduct(pathToCsv, pathToCsvByBrand);
		for (Product prod : csv.getProducts()) {
			Namespace personsBucket = new Namespace("product");
			Location personLocation = new Location(personsBucket, prod.getAsin());
			StoreValue storePersonOp = new StoreValue.Builder(prod).withLocation(personLocation).build();
			client.execute(storePersonOp);
		}
	}

	private static void storeVendor(String pathToCsv, RiakClient client)
			throws IOException, ParseException, ExecutionException, InterruptedException {
		Csv csv = new Csv();
		csv.readVendor(pathToCsv);
		for (Vendor vend : csv.getVendors()) {
			Namespace personsBucket = new Namespace("vendor");
			Location personLocation = new Location(personsBucket, vend.getVendor());
			StoreValue storePersonOp = new StoreValue.Builder(vend).withLocation(personLocation).build();
			client.execute(storePersonOp);
		}
	}

	private static void storeInvoice(String pathToCsv, RiakClient client)
			throws ExecutionException, InterruptedException {
		Xml xml = new Xml();
		xml.readInvoice(pathToCsv);
		for (Order inv : xml.getInvoices()) {
			try {// Object too heavy ( #samuel )
				Namespace personsBucket = new Namespace("invoice");
				Location personLocation = new Location(personsBucket, inv.getOrderId());
				StoreValue storePersonOp = new StoreValue.Builder(inv).withLocation(personLocation).build();
				client.execute(storePersonOp);
			} catch (Exception e) {
//				e.printStackTrace();
			}
		}
	}

	private static void storeOrder(String pathToCsv, RiakClient client)
			throws IOException, ParseException, ExecutionException, InterruptedException, RiakResponseException {
		Json json = new Json();
		json.readOrder(pathToCsv);
		for (Order jayson : json.getOrders()) {
			try {// Object too large ( #ouali )
				Namespace personsBucket = new Namespace("order");
				Location personLocation = new Location(personsBucket, "gdsgfg");
				StoreValue storePersonOp = new StoreValue.Builder(jayson).withLocation(personLocation).build();
				client.execute(storePersonOp);
			} catch (Exception e) {

			}
		}
	}

	public static void main(String[] args) {
		try {

			RiakCluster cluster = setUpCluster();
			RiakClient client = new RiakClient(cluster);
			System.out.println("Client object successfully created");

//			storePerson(PATH + "Customer/person_0_0.csv",client);
//			storeFeedback(PATH + "Feedback/Feedback.csv",client);
//			storeProduct(PATH + "Product/Product.csv","/home/loubard/Documents/BD_BigTable/DATA/Product/BrandByProduct.csv",client);
//			storeVendor(PATH + "Vendor/Vendor.csv",client);
//			storeInvoice(PATH + "Invoice/Invoice.xml", client);
			storeOrder(PATH + "Order/Order.json", client);

//
//			Namespace personsBucket = new Namespace("invoice");
//			Location personLocation = new Location(personsBucket, "4da0a2a0-770d-479d-b48f-dcfab4a33e7c");
//			FetchValue fetchMobyDickOp = new FetchValue.Builder(personLocation).build();
//			Order fetchedBook = client.execute(fetchMobyDickOp).getValue(Order.class);
//			System.out.println(fetchedBook.getTotalPrice());
//
//			Namespace animalsBucket = new Namespace("invoice");
//			FetchBucketProperties fetchProps = new FetchBucketProperties.Builder(animalsBucket).build();
//			Response response = client.execute(fetchProps);
//			BucketProperties props = response.getBucketProperties();
//			System.out.println(props);

			Json json = new Json();
			json.readOrder("/home/loubard/Documents/BD_BigTable/DATA/Order/Order.json");

			cluster.shutdown();

		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
}