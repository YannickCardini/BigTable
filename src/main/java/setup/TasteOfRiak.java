package main.java.setup;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.kv.UpdateValue;
import com.basho.riak.client.api.commands.search.StoreIndex;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.netty.RiakResponseException;
import com.basho.riak.client.core.operations.SearchOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.search.YokozunaIndex;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.basho.riak.client.*;

public class TasteOfRiak {
	// A basic POJO class to demonstrate typed exchanges with Riak
	// public static final String PATH = "/home/benoit/Documents/ressources/";
	public static final String PATH = "/home/loubard/Documents/BD_BigTable/DATA/";

	// TODO : modifier le nom du dossier #YannickMiage
	public static final String PROJECT_PATH = ToolBox.getProjectDirectoryPath();
	public static String dataPath;

	// get Data directory
	private static String getDataDirectory() {
		return dataPath;
	}

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
		double csvLength = csv.getPersons().size();
		double i = 0;

		for (Person pers : csv.getPersons()) {
			Location loc;
			RiakObject ro;
			StoreValue storeOp;
			Namespace bucket = new Namespace("person", pers.getId());

			if (pers.getBirthday() != null && pers.getBirthday().length() > 0) {
				loc = new Location(bucket, "birthday");
				ro = new RiakObject();
				ro.setValue(BinaryValue.create(pers.getBirthday()));
				storeOp = new StoreValue.Builder(ro).withLocation(loc).build();
				try {
					client.execute(storeOp);
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}

			if (pers.getFirstName() != null && pers.getFirstName().length() > 0) {
				loc = new Location(bucket, "firstName");
				ro = new RiakObject();
				ro.setValue(BinaryValue.create(pers.getFirstName()));
				storeOp = new StoreValue.Builder(ro).withLocation(loc).build();
				try {
					client.execute(storeOp);
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}

			if (pers.getLastName() != null && pers.getLastName().length() > 0) {
				loc = new Location(bucket, "lastName");
				ro = new RiakObject();
				ro.setValue(BinaryValue.create(pers.getLastName()));
				storeOp = new StoreValue.Builder(ro).withLocation(loc).build();
				try {
					client.execute(storeOp);
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}

			if (pers.getGender() != null && pers.getGender().length() > 0) {
				loc = new Location(bucket, "gender");
				ro = new RiakObject();
				ro.setValue(BinaryValue.create(pers.getGender()));
				storeOp = new StoreValue.Builder(ro).withLocation(loc).build();
				try {
					client.execute(storeOp);
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}

			if (pers.getCreateDate() != null && pers.getCreateDate().length() > 0) {
				loc = new Location(bucket, "createDate");
				ro = new RiakObject();
				ro.setValue(BinaryValue.create(pers.getCreateDate()));
				storeOp = new StoreValue.Builder(ro).withLocation(loc).build();
				try {
					client.execute(storeOp);
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}

			if (pers.getLocation() != null && pers.getLocation().length() > 0) {
				loc = new Location(bucket, "location");
				ro = new RiakObject();
				ro.setValue(BinaryValue.create(pers.getLocation()));
				storeOp = new StoreValue.Builder(ro).withLocation(loc).build();
				try {
					client.execute(storeOp);
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}

			if (pers.getBrowserUsed() != null && pers.getBrowserUsed().length() > 0) {
				loc = new Location(bucket, "browserUsed");
				ro = new RiakObject();
				ro.setValue(BinaryValue.create(pers.getBrowserUsed()));
				storeOp = new StoreValue.Builder(ro).withLocation(loc).build();
				try {
					client.execute(storeOp);
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}

			if (pers.getPlace() != null && pers.getPlace().length() > 0) {
				loc = new Location(bucket, "place");
				ro = new RiakObject();
				ro.setValue(BinaryValue.create(pers.getPlace()));
				storeOp = new StoreValue.Builder(ro).withLocation(loc).build();
				try {
					client.execute(storeOp);
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}

			i++;
			System.out.println(String.format("%.2f", (i / csvLength) * 100) + "%");
		}
	}

	private static void storeFeedback(String pathToCsv, RiakClient client)
			throws IOException, ParseException, ExecutionException, InterruptedException {
		Csv csv = new Csv();
		csv.readFeedback(pathToCsv);
		double csvLength = csv.getFeedbacks().size();
		double i = 0;
		for (Feedback feed : csv.getFeedbacks()) {
			Location loc;
			RiakObject ro;
			StoreValue storeOp;
			Namespace bucket = new Namespace("feedback", feed.getAsin() + feed.getPersonId());// Combo asin +personId
																								// car aucun des 2
																								// uniques

			if (feed.getFeedback() != null && feed.getFeedback().length() > 0) {
				loc = new Location(bucket, "feed");
				ro = new RiakObject();
				ro.setValue(BinaryValue.create(feed.getFeedback()));
				storeOp = new StoreValue.Builder(ro).withLocation(loc).build();
				try {
					client.execute(storeOp);
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}

			i++;
			System.out.println(String.format("%.2f", (i / csvLength) * 100) + "%");
		}

	}

	private static void storeProduct(String pathToCsv, String pathToCsvByBrand, RiakClient client)
			throws IOException, ParseException, InterruptedException {
		Csv csv = new Csv();
		csv.readProduct(pathToCsv, pathToCsvByBrand);
		double csvLength = csv.getProducts().size();
		double i = 0;
		for (Product prod : csv.getProducts()) {
			Location productLocation;
			RiakObject ro;
			StoreValue storeProductOp;
			Namespace productBucket = new Namespace("product", prod.getAsin());

			if (prod.getPrice() != null && prod.getPrice().length() > 0) {
				productLocation = new Location(productBucket, "price");
				ro = new RiakObject();
				ro.setValue(BinaryValue.create(prod.getPrice()));
				storeProductOp = new StoreValue.Builder(ro).withLocation(productLocation).build();
				try {
					client.execute(storeProductOp);
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}

			if (prod.getTitle() != null && prod.getTitle().length() > 0) {
				productLocation = new Location(productBucket, "title");
				ro = new RiakObject();
				ro.setValue(BinaryValue.create(prod.getTitle()));
				storeProductOp = new StoreValue.Builder(ro).withLocation(productLocation).build();
				try {
					client.execute(storeProductOp);
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}

			if (prod.getImgUrl() != null && prod.getImgUrl().length() > 0) {
				productLocation = new Location(productBucket, "imgUrl");
				ro = new RiakObject();
				ro.setValue(BinaryValue.create(prod.getImgUrl()));
				storeProductOp = new StoreValue.Builder(ro).withLocation(productLocation).build();
				try {
					client.execute(storeProductOp);
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}

			if (prod.getBrand() != null && prod.getBrand().length() > 0) {
				productLocation = new Location(productBucket, "brand");
				ro = new RiakObject();
				ro.setValue(BinaryValue.create(prod.getBrand()));
				storeProductOp = new StoreValue.Builder(ro).withLocation(productLocation).build();
				try {
					client.execute(storeProductOp);
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}
			i++;
			System.out.println(String.format("%.2f", (i / csvLength) * 100) + "%");
		}
	}

	private static void updateProduct(String bucketName, String productKey, String newValue, RiakClient client)
			throws IOException, ParseException, ExecutionException, InterruptedException {
		Location productLocation = new Location(new Namespace("product", bucketName), productKey);
		FetchValue fetch = new FetchValue.Builder(productLocation).build();
		FetchValue.Response response = client.execute(fetch);
		RiakObject obj = response.getValue(RiakObject.class);
		obj.setValue(BinaryValue.create(newValue));
		/*
		 * UpdateValue updateOp = new UpdateValue.Builder(productLocation)
		 * .withUpdate(UpdateValue.Update.clobberUpdate(brandNewUser)) .build();
		 * client.execute(updateOp);
		 */
		System.out.println("Opération de mise à jour réussie");
	}

	private static void storeVendor(String pathToCsv, RiakClient client)
			throws IOException, ParseException, ExecutionException, InterruptedException {
		Csv csv = new Csv();
		csv.readVendor(pathToCsv);
		double csvLength = csv.getVendors().size();
		double i = 0;
		for (Vendor vend : csv.getVendors()) {
			Location vendorLocation;
			RiakObject ro;
			StoreValue storeOp;
			Namespace bucket = new Namespace("vendor", vend.getVendor());

			if (vend.getCountry() != null && vend.getCountry().length() > 0) {
				vendorLocation = new Location(bucket, "country");
				ro = new RiakObject();
				ro.setValue(BinaryValue.create(vend.getCountry()));
				storeOp = new StoreValue.Builder(ro).withLocation(vendorLocation).build();
				try {
					client.execute(storeOp);
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}


			if (vend.getIndustry() != null && vend.getIndustry().length() > 0) {
				vendorLocation = new Location(bucket, "industry");
				ro = new RiakObject();
				ro.setValue(BinaryValue.create(vend.getIndustry()));
				storeOp = new StoreValue.Builder(ro).withLocation(vendorLocation).build();
				try {
					client.execute(storeOp);
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}

			i++;
			System.out.println(String.format("%.2f", (i / csvLength) * 100) + "%");
		}
	}

	private static void storeInvoice(String pathToXml, RiakClient client)
			throws ExecutionException, InterruptedException {
		Xml xml = new Xml();
		xml.readInvoice(pathToXml);
		double csvLength = xml.getInvoices().size();
		double i = 0;
		for (Order inv : xml.getInvoices()) {
			Location vendorLocation;
			RiakObject ro;
			StoreValue storeOp;
			Namespace bucket = new Namespace("invoice", inv.getOrderId());

			if (inv.getPersonId() != null && inv.getPersonId().length() > 0) {
				vendorLocation = new Location(bucket, "personId");
				ro = new RiakObject();
				ro.setValue(BinaryValue.create(inv.getPersonId()));
				storeOp = new StoreValue.Builder(ro).withLocation(vendorLocation).build();
				try {
					client.execute(storeOp);
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}

			if (inv.getOrderDate() != null && inv.getOrderDate().length() > 0) {
				vendorLocation = new Location(bucket, "orderDate");
				ro = new RiakObject();
				ro.setValue(BinaryValue.create(inv.getOrderDate()));
				storeOp = new StoreValue.Builder(ro).withLocation(vendorLocation).build();
				try {
					client.execute(storeOp);
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}

			if (inv.getTotalPrice() != null && inv.getTotalPrice().length() > 0) {
				vendorLocation = new Location(bucket, "totalPrice");
				ro = new RiakObject();
				ro.setValue(BinaryValue.create(inv.getTotalPrice()));
				storeOp = new StoreValue.Builder(ro).withLocation(vendorLocation).build();
				try {
					client.execute(storeOp);
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}
			
			if (inv.getOrderLine() != null && inv.getOrderLine().length() > 0) {
				vendorLocation = new Location(bucket, "orderLine");
				ro = new RiakObject();
				ro.setValue(BinaryValue.create(inv.getOrderLine()));
				storeOp = new StoreValue.Builder(ro).withLocation(vendorLocation).build();
				try {
					client.execute(storeOp);
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}


			i++;
			System.out.println(String.format("%.2f", (i / csvLength) * 100) + "%");
		}
	}

	private static void storeOrder(String pathToJson, RiakClient client)
			throws IOException, ParseException, ExecutionException, InterruptedException, RiakResponseException {
		Json json = new Json();
		json.readOrder(pathToJson);
		double csvLength = json.getOrders().size();
		double i = 0;
		for (Order ord : json.getOrders()) {
			Location vendorLocation;
			RiakObject ro;
			StoreValue storeOp;
			Namespace bucket = new Namespace("order", ord.getOrderId());

			if (ord.getPersonId() != null && ord.getPersonId().length() > 0) {
				vendorLocation = new Location(bucket, "personId");
				ro = new RiakObject();
				ro.setValue(BinaryValue.create(ord.getPersonId()));
				storeOp = new StoreValue.Builder(ro).withLocation(vendorLocation).build();
				try {
					client.execute(storeOp);
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}

			if (ord.getOrderDate() != null && ord.getOrderDate().length() > 0) {
				vendorLocation = new Location(bucket, "orderDate");
				ro = new RiakObject();
				ro.setValue(BinaryValue.create(ord.getOrderDate()));
				storeOp = new StoreValue.Builder(ro).withLocation(vendorLocation).build();
				try {
					client.execute(storeOp);
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}

			if (ord.getTotalPrice() != null && ord.getTotalPrice().length() > 0) {
				vendorLocation = new Location(bucket, "totalPrice");
				ro = new RiakObject();
				ro.setValue(BinaryValue.create(ord.getTotalPrice()));
				storeOp = new StoreValue.Builder(ro).withLocation(vendorLocation).build();
				try {
					client.execute(storeOp);
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}
			
			if (ord.getOrderLine() != null && ord.getOrderLine().length() > 0) {
				vendorLocation = new Location(bucket, "orderLine");
				ro = new RiakObject();
				ro.setValue(BinaryValue.create(ord.getOrderLine()));
				storeOp = new StoreValue.Builder(ro).withLocation(vendorLocation).build();
				try {
					client.execute(storeOp);
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}

			i++;
			System.out.println(String.format("%.2f", (i / csvLength) * 100) + "%");
		}
	}

	public static void main(String[] args) {
		try {

			RiakCluster cluster = setUpCluster();
			RiakClient client = new RiakClient(cluster);
			System.out.println("Client object successfully created");
//			
//			Json json = new Json();
//			json.fillRiakWithJsonFile(PATH + "tmp.json", client);

			/////////////////////////// Alimente la BDD, extremement long et a usage
			/////////////////////////// unique///////////////////////
//			 storePerson(PATH + "Customer/person_0_0.csv",client); //
//			 storeFeedback(PATH + "Feedback/Feedback.csv",client); //
//			 storeProduct(PATH + "Product/Product.csv", PATH + "Product/BrandByProduct.csv", client); //
//			 storeVendor(PATH + "Vendor/Vendor.csv",client); //
//			 storeInvoice(PATH + "Invoice/Invoice.xml", client); //
//			 storeOrder(PATH + "Order/Order.json", client); //
			////////////////////////////////////////////////////////////////////////////////////////////////////

			// Alimentation
//			Namespace animalsBucket = new Namespace("order");
//			String json = "application/json";
//
//			RiakObject liono = new RiakObject()
//			        .setContentType(json)
//			        .setValue(BinaryValue.create("{\"OrderId\":\"oui\",\"age_i\":30,\"leader_b\":true}"));
//			Location lionoLoc = new Location(animalsBucket, "liono");
//
//			StoreValue lionoStore = new StoreValue.Builder(liono).withLocation(lionoLoc).build();
//			// The other StoreValue operations can be built the same way
//
//			client.execute(lionoStore);
//			
			// Search
			SearchOperation searchOp = new SearchOperation
			        .Builder(BinaryValue.create("searchOrder"), "*:*")
			        .build();
			cluster.execute(searchOp);
			// This will display the actual results as a List of Maps:
			List<Map<String, List<String>>> results = searchOp.get().getAllResults();
			// This will display the number of results:
			System.out.println(results);
			

			cluster.shutdown();

		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
}
