package main.java.query;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import main.java.documents.Product;

/**
 * Classe dédiée aux query, elle implémente deux query :
 * 
 * •	Query 2. For a given product during a given period, find the people who commented or posted on it, and had bought it.
 * •	Query 8. For all the products of a given category during a given year, compute its total sales amount, and measure its popularity in the social media.
 * 
 */
public class QueryApp {

	private RiakCluster cluster;
	private RiakClient client;
	
	/**
	 * Constructor with Cluster of our database
	 * @param rc instance of RiakCluster
	 */
	public QueryApp(RiakCluster cluster, RiakClient client) {
		this.cluster = cluster;
		this.client = client;
	}
	
	/**
	 * Query 2
	 * @param ProductName le nom du produit
	 * @param begin la date de début au format yyyy-MM-dd
	 * @param end la date de fin au format yyyy-MM-dd
	 * @throws ParseException 
	 * @throws InterruptedException 
	 * @throws ExecutionException 
	 */
	public void getPeopleWithAsinAndPeriod(String asin, String begin, String end) throws ParseException {
		
		//Les periodes
		Date dBegin;
		Date dEnd;
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        dBegin = formatter.parse(begin);
        dEnd = formatter.parse(end);
		
		// product : asin 
		Location productLoc = new Location(new Namespace("product", asin), "asin");
		FetchValue fetchProductLoc = new FetchValue.Builder(productLoc).build();
		RiakObject objResponseFetchProductLoc = null;
		
		try {
			
			// On exécute
			FetchValue.Response responseFetchProductLoc = client.execute(fetchProductLoc);		
			// On récupère l'objet
			objResponseFetchProductLoc = responseFetchProductLoc.getValue(RiakObject.class);
			
		} catch (ExecutionException | InterruptedException e) {
			e.printStackTrace();
		}
		
		if(objResponseFetchProductLoc != null) {
			
			Namespace invoicePersonIdSpace = new Namespace("invoice", objResponseFetchProductLoc.getValue().toString());
			Location invoicePersonIdLoc = new Location(invoicePersonIdSpace, "personId");
			Location invoiceOrderDateLoc = new Location(invoicePersonIdSpace, "orderDate");
			
			FetchValue fetchInvoicePersonIdLoc = new FetchValue.Builder(invoicePersonIdLoc).build();
			FetchValue fetchInvoiceOrderDateLoc = new FetchValue.Builder(invoiceOrderDateLoc).build();
			
			List<RiakObject> objResponseFetchInvoicePersonIdLoc = null;
			List<RiakObject> objResponseFetchInvoiceOrderDateLoc = null;
			
			try {
				
				// On exécute
				FetchValue.Response responseFetchInvoicePersonIdLoc = client.execute(fetchInvoicePersonIdLoc);
				FetchValue.Response responseFetchInvoiceOrderDateLoc = client.execute(fetchInvoiceOrderDateLoc);
				// On récupère l'objet
				objResponseFetchInvoicePersonIdLoc = responseFetchInvoicePersonIdLoc.getValues();
				objResponseFetchInvoiceOrderDateLoc = responseFetchInvoiceOrderDateLoc.getValues();
				
			} catch (ExecutionException | InterruptedException e) {
				e.printStackTrace();
			}
			
			if(objResponseFetchInvoicePersonIdLoc != null && objResponseFetchInvoiceOrderDateLoc != null) {
				
				List<RiakObject> listObjResponseFetchFeedbackFeedbackLoc = new ArrayList<RiakObject>();
				List<RiakObject> listObjResponseFetchFeedbackPersonIdLoc = new ArrayList<RiakObject>();
				
				for(RiakObject ro : objResponseFetchInvoicePersonIdLoc) {
					for(RiakObject rod : objResponseFetchInvoiceOrderDateLoc) {
						Date dateOfRod = formatter.parse(rod.getValue().toString());
						if(dateOfRod.before(dEnd) && dateOfRod.after(dBegin)) {
							
							// feedback : asin , feedback , personId
							Namespace feedbackForAsin = new Namespace("feedback", objResponseFetchProductLoc.getValue().toString() + ro.getValue().toString());
							Location feedbackFeedbackLoc = new Location(feedbackForAsin, "feedback");
							Location feedbackPersonIdLoc = new Location(feedbackForAsin, "personId");
							// Fetch
							FetchValue fetchFeedbackFeedbackLoc = new FetchValue.Builder(feedbackFeedbackLoc).build();
							FetchValue fetchFeedbackPersonIdLoc = new FetchValue.Builder(feedbackPersonIdLoc).build();
							
							RiakObject objResponseFetchFeedbackFeedbackLoc = null;
							RiakObject objResponseFetchFeedbackPersonIdLoc = null;
							
							try {
								
								// On exécute
								FetchValue.Response responseFetchFeedbackFeedbackLoc = client.execute(fetchFeedbackFeedbackLoc);
								FetchValue.Response responseFetchFeedbackPersonIdLoc = client.execute(fetchFeedbackPersonIdLoc);
								// On récupère les objets
								objResponseFetchFeedbackFeedbackLoc = responseFetchFeedbackFeedbackLoc.getValue(RiakObject.class);
								objResponseFetchFeedbackPersonIdLoc = responseFetchFeedbackPersonIdLoc.getValue(RiakObject.class);
								
								if(objResponseFetchFeedbackFeedbackLoc != null && objResponseFetchFeedbackPersonIdLoc != null) {
									listObjResponseFetchFeedbackFeedbackLoc.add(objResponseFetchFeedbackFeedbackLoc);
									listObjResponseFetchFeedbackPersonIdLoc.add(objResponseFetchFeedbackPersonIdLoc);
								} else {
									System.out.println("[QUERY2] - Feedback : aucun feedback trouvé !");
								}
								
							} catch (ExecutionException | InterruptedException e) {
								e.printStackTrace();
							}
							
						}
					}
				}
				
				// On parcours nos listes pour relever les personnes qui ont commentés
				if(!listObjResponseFetchFeedbackFeedbackLoc.isEmpty() && !listObjResponseFetchFeedbackPersonIdLoc.isEmpty()) {
					
					for(RiakObject ro : listObjResponseFetchFeedbackPersonIdLoc) {
						
						Namespace personSpace = new Namespace("person", ro.getValue().toString());
						Location personIdLoc = new Location(personSpace, "id");
						Location personFirstNameLoc = new Location(personSpace, "firstName");
						Location personLastNameLoc = new Location(personSpace, "lastName");
						// Fetch
						FetchValue fetchPersonIdLoc = new FetchValue.Builder(personIdLoc).build();
						FetchValue fetchPersonFirstNameLoc = new FetchValue.Builder(personFirstNameLoc).build();
						FetchValue fetchPersonLastNameLoc = new FetchValue.Builder(personLastNameLoc).build();
						
						try {
							
							FetchValue.Response responseFetchPersonIdLoc = client.execute(fetchPersonIdLoc);
							FetchValue.Response responseFetchPersonFirstNameLoc = client.execute(fetchPersonFirstNameLoc);
							FetchValue.Response responseFetchPersonLastNameLoc = client.execute(fetchPersonLastNameLoc);
							
							// LES RESULTATS
							System.out.println("[RES] - person {id: " + responseFetchPersonIdLoc.getValue(RiakObject.class).getValue().toString() +
												"; firstName: " + responseFetchPersonFirstNameLoc.getValue(RiakObject.class).getValue().toString() +
												"; lastName: " + responseFetchPersonLastNameLoc.getValue(RiakObject.class).getValue().toString());
							
						} catch (ExecutionException | InterruptedException e) {
							e.printStackTrace();
						}
						
					}	
					
				}
				
			} else {
				System.out.println("[QUERY2] - Feedback : introuvable avec les parametres suivants : " + asin + "; begin: " + begin + "; end: " + end);
			}
			
		} else {
			System.out.println("[QUERY2] - Product : produit introuvable pour l'asin : " + asin);
		}

	}
}
