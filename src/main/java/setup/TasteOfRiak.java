package main.java.setup;


import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.kv.UpdateValue;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.netty.RiakResponseException;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import main.java.documents.*;
import main.java.read.Csv;
import main.java.read.Json;
import main.java.read.Xml;
import main.java.util.ToolBox;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.concurrent.ExecutionException;

import com.basho.riak.client.*;

public class TasteOfRiak {
	// A basic POJO class to demonstrate typed exchanges with Riak
	public static final String PATH = "/home/benoit/Documents/ressources/";

	// TODO : modifier le nom du dossier #YannickMiage
	public static final String PROJECT_PATH = ToolBox.getProjectDirectoryPath();
	public static String dataPath;
	
	//get Data directory
	private static String getDataDirectory() {
		return dataPath;
	}
	
	// This will create a client object that we can use to interact with Riak
	private static RiakCluster setUpCluster() throws UnknownHostException {

		// This example will use only one node listening on localhost:10017
		RiakNode node = new RiakNode.Builder().withRemoteAddress("18.220.122.237").build();

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
			System.out.println(per);

			client.execute(storePersonOp);

		}
	}

	private static void storeFeedback(String pathToCsv, RiakClient client)
			throws IOException, ParseException, ExecutionException, InterruptedException {
		Csv csv = new Csv();
		csv.readFeedback(pathToCsv);
		double csvLength = csv.getFeedbacks().size();
		double i = 0;
		for (Feedback feed : csv.getFeedbacks()) {
			Namespace personsBucket = new Namespace("feedback");
			Location personLocation = new Location(personsBucket, feed.getAsin() + feed.getPersonId());// Combo asin +
																										// personId car
																										// aucun des 2
																										// uniques
			StoreValue storePersonOp = new StoreValue.Builder(feed).withLocation(personLocation).build();
			client.execute(storePersonOp);
			i++;
			System.out.println(String.format("%.2f", (i / csvLength) * 100 ) + "%");
		}
	}

	private static void storeProduct(String pathToCsv, String pathToCsvByBrand, RiakClient client)
			throws IOException, ParseException, InterruptedException {
		Csv csv = new Csv();
		csv.readProduct(pathToCsv, pathToCsvByBrand);
		double csvLength = csv.getProducts().size();
		double i = 0;
		for (Product prod : csv.getProducts()) {
			Namespace personsBucket = new Namespace("product", prod.getAsin());
			Location personLocation = new Location(personsBucket, "price");
			RiakObject ro= new RiakObject();
			ro.setValue(BinaryValue.create(prod.getPrice().toString()));
System.out.println(prod.getAsin());
			StoreValue storePersonOp = new StoreValue.Builder(ro).withLocation(personLocation).build();
			try {
				client.execute(storePersonOp);
			} catch (ExecutionException e) {
				e.printStackTrace();
			}

			personsBucket = new Namespace("product", prod.getAsin());
			personLocation = new Location(personsBucket, "title");
			ro= new RiakObject();
			ro.setValue(BinaryValue.create(prod.getTitle()));

			storePersonOp = new StoreValue.Builder(ro).withLocation(personLocation).build();
			try {
				client.execute(storePersonOp);
			} catch (ExecutionException e) {
				e.printStackTrace();
			}

			personsBucket = new Namespace("product", prod.getAsin());
			personLocation = new Location(personsBucket, "imgUrl");
			ro= new RiakObject();
			ro.setValue(BinaryValue.create(prod.getImgUrl()));

			storePersonOp = new StoreValue.Builder(ro).withLocation(personLocation).build();
			try {
				client.execute(storePersonOp);
			} catch (ExecutionException e) {
				e.printStackTrace();
			}

			personsBucket = new Namespace("product", prod.getAsin());
			personLocation = new Location(personsBucket, "brand");
			ro= new RiakObject();
			ro.setValue(BinaryValue.create(prod.getBrand()));
			storePersonOp = new StoreValue.Builder(ro).withLocation(personLocation).build();
			try {
				client.execute(storePersonOp);
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
	}

	private static void updateProduct(String bucketName, String productKey, String newValue, RiakClient client)
			throws IOException, ParseException, ExecutionException, InterruptedException {
		Location productLocation = new Location(new Namespace("product", bucketName), productKey);
		FetchValue fetch = new FetchValue.Builder(productLocation)
				.build();
		FetchValue.Response response = client.execute(fetch);
		RiakObject obj = response.getValue(RiakObject.class);
		obj.setValue(BinaryValue.create(newValue));
		/*UpdateValue updateOp = new UpdateValue.Builder(productLocation)
				.withUpdate(UpdateValue.Update.clobberUpdate(brandNewUser))
				.build();
		client.execute(updateOp);*/
		System.out.println("Opération de mise à jour réussie");
	}

	private static void storeVendor(String pathToCsv, RiakClient client)
			throws IOException, ParseException, ExecutionException, InterruptedException {
		Csv csv = new Csv();
		csv.readVendor(pathToCsv);
		double csvLength = csv.getVendors().size();
		double i = 0;
		for (Vendor vend : csv.getVendors()) {
			Namespace personsBucket = new Namespace("vendor");
			Location personLocation = new Location(personsBucket, vend.getVendor());
			StoreValue storePersonOp = new StoreValue.Builder(vend).withLocation(personLocation).build();
			client.execute(storePersonOp);
			i++;
			System.out.println(String.format("%.2f", (i / csvLength) * 100 ) + "%");
		}
	}

	private static void storeInvoice(String pathToCsv, RiakClient client)
			throws ExecutionException, InterruptedException {
		Xml xml = new Xml();
		xml.readInvoice(pathToCsv);
		double xmlLength = xml.getInvoices().size();
		double i = 0;
		for (Order inv : xml.getInvoices()) {
			try {// Object really heavy
				Namespace personsBucket = new Namespace("invoice");
				Location personLocation = new Location(personsBucket, inv.getOrderId());
				StoreValue storePersonOp = new StoreValue.Builder(inv).withLocation(personLocation).build();
				client.execute(storePersonOp);
				i++;
				System.out.println(String.format("%.2f", (i / xmlLength) * 100 ) + "%");
			} catch (Exception e) {
//				e.printStackTrace();
			}
		}
	}

	private static void storeOrder(String pathToJson, RiakClient client)
			throws IOException, ParseException, ExecutionException, InterruptedException, RiakResponseException {
		Json json = new Json();
		json.readOrder(pathToJson);
		double jsonLength = json.getOrders().size();
		double i = 0;
		for (Order jayson : json.getOrders()) {
			try {// Object really large
				Namespace personsBucket = new Namespace("order");
				Location personLocation = new Location(personsBucket, "gdsgfg");
				StoreValue storePersonOp = new StoreValue.Builder(jayson).withLocation(personLocation).build();
				client.execute(storePersonOp);
				i++;
				System.out.println(String.format("%.2f", (i / jsonLength) * 100 ) + "%");
			} catch (Exception e) {
			}
		}
	}

	public static void main(String[] args) {
		try {

			RiakCluster cluster = setUpCluster();
			RiakClient client = new RiakClient(cluster);
			System.out.println("Client object successfully created");

/////////////////////////// Alimente la BDD, extremement long et a usage unique///////////////////////
//			storePerson(PATH + "Customer/person_0_0.csv",client);									//
//			storeFeedback(PATH + "Feedback/Feedback.csv",client);									//
//			storeProduct(PATH + "Product/Product.csv",PATH + "Product/BrandByProduct.csv",client);	//
//			storeVendor(PATH + "Vendor/Vendor.csv",client);											//
//			storeInvoice(PATH + "Invoice/Invoice.xml", client);										//
//			storeOrder(PATH + "Order/Order.json", client);											//
//////////////////////////////////////////////////////////////////////////////////////////////////////
//			storePerson(PATH + "Customer/person_0_0.csv",client);
//			storeFeedback(PATH + "Feedback/Feedback.csv",client);

					//Test d'insertion de tous les Product
			//String productPath = PROJECT_PATH+"/ressources/Product" + ToolBox.SEPARATOR + "Product.csv";
			//File op = ToolBox.getFileIntoRessources(productPath);

			//String product2Path = PROJECT_PATH+"/ressources/Product" + ToolBox.SEPARATOR + "BrandByProduct.csv";
			//File op2 = ToolBox.getFileIntoRessources(product2Path);

			//storeProduct(productPath, product2Path, client);


	//
//			Namespace personsBucket = new Namespace("invoice");
//			Location personLocation = new Location(personsBucket, "4da0a2a0-770d-479d-b48f-dcfab4a33e7c");
//			FetchValue fetchMobyDickOp = new FetchValue.Builder(personLocation).build();
//			Order fetchedBook = client.execute(fetchMobyDickOp).getValue(Order.class);
//			System.out.println(fetchedBook.getTotalPrice());
	////			Namespace animalsBucket = new Namespace("invoice");
//			FetchBucketProperties fetchProps = new FetchBucketProperties.Builder(animalsBucket).build();
//			Response response = client.execute(fetchProps);
//			BucketProperties props = response.getBucketProperties();
//			System.out.println(props);

			/*Json json = new Json();
			json.readOrder(op);*/

			//Test de modification d'une donnée (à terminer)
/*Location myKey = new Location(new Namespace("product","2094869245"), "price");
FetchValue fetch = new FetchValue.Builder(myKey)
        .build();
FetchValue.Response response = client.execute(fetch);
RiakObject obj = response.getValue(RiakObject.class);
System.out.println("Ancien objet: "+obj.getValue());

updateProduct("2094869245", "title","test",client);

			myKey = new Location(new Namespace("product","2094869245"), "price");
			fetch = new FetchValue.Builder(myKey)
					.build();
			response = client.execute(fetch);
			obj = response.getValue(RiakObject.class);
			System.out.println("Nouvel objet: "+obj.getValue());*/
			cluster.shutdown();

		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
}
