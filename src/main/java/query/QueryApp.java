package main.java.query;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.operations.SearchOperation;
import com.basho.riak.client.core.util.BinaryValue;

/**
 * Classe dédiée aux query, elle implémente deux query :
 * 
 * •	Query 2. For a given product during a given period, find the people who commented or posted on it, and had bought it.
 * •	Query 8. For all the products of a given category during a given year, compute its total sales amount, and measure its popularity in the social media.
 * 
 */
public class QueryApp {

	private RiakClient client;
	private RiakCluster cluster;
	
	public QueryApp(RiakClient client, RiakCluster cluster) {
		this.client = client;
		this.cluster = cluster;
	}
	
	public Date[] getPeriod(String begin, String end) {
		Date[] dates = new Date[2];
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        try {
			dates[0] = formatter.parse(begin);
			dates[1] = formatter.parse(end);
		} catch (ParseException e) {
			System.out.println("[ERROR] - Conversion impossible pour les dates suivantes: " + begin + " ; " + end);
			return null;
		}
		return dates;
	}
	
	public Date getDateFromString(String date) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		Date res = null;
		try {
			res = formatter.parse(date);
		} catch (ParseException e) {
			System.out.println("[ERROR] - Conversion impossible pour la date suivante: " + date);
			return res;
		}
		return res;
	}
	
	/**
	 * Query 2
	 * @param asin l'asin du produit
	 * @param begin la date de début au format yyyy-MM-dd
	 * @param end la date de fin au format yyyy-MM-dd
	 * @throws ParseException 
	 */
	public void getPeopleForAsinAndPeriod(String asin, String begin, String end) throws ParseException {
		
		Date[] period = getPeriod(begin, end);
		
		if(period != null) {
			// On recherche toutes les factures liées aux produits dont l'asin est spécifié
			SearchOperation searchInvoice = new SearchOperation.Builder(BinaryValue.create("searchInvoice"), "orderLine:*asin:" + asin + "*").build();
			cluster.execute(searchInvoice);
			List<Map<String, List<String>>> resultsInvoice = new ArrayList<Map<String,List<String>>>();
			List<Map<String, List<String>>> resultsInvoiceFinal = new ArrayList<Map<String,List<String>>>();
			try {
				resultsInvoice = searchInvoice.get().getAllResults();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
			if(!resultsInvoice.isEmpty()) {
				for(Map<String, List<String>> list : resultsInvoice) {
					String date = list.get("orderDate").get(0);
					// Si notre date est incluse dans la période souhaitée
					if(getDateFromString(date).after(period[0]) && getDateFromString(date).before(period[1])) {
						resultsInvoiceFinal.add(list);
					}
				}
				// Maintenant que nous avons affiné la liste, on peut chercher les individus liés à l'achat de celui-ci
				for(Map<String, List<String>> list : resultsInvoiceFinal) {
					String personId = list.get("personId").get(0);
					// On cherche pour ces individus, leur nom et prénom
					SearchOperation searchPerson = new SearchOperation.Builder(BinaryValue.create("searchPerson"), "personId:" + personId).build();
					cluster.execute(searchPerson);
					List<Map<String, List<String>>> resultsPerson = new ArrayList<Map<String,List<String>>>();
					try {
						resultsPerson = searchPerson.get().getAllResults();
					} catch (InterruptedException | ExecutionException e) {
						e.printStackTrace();
					}
					if(!resultsPerson.isEmpty()) {
						List<Map<String, List<String>>> resultsPersonFinal = new ArrayList<Map<String,List<String>>>();
						for(Map<String, List<String>> l : resultsPerson) {
							//Est-ce que ces personnes ont émis un feedback de ce produit ?
							String id = l.get("personId").get(0);
							String firstName = l.get("firstName").get(0);
							String lastName = l.get("lastName").get(0);
							SearchOperation searchFeedback = new SearchOperation.Builder(BinaryValue.create("searchFeedback"), "personId:" + personId).build();
							cluster.execute(searchFeedback);
							List<Map<String, List<String>>> resultsFeedback = new ArrayList<Map<String,List<String>>>();
							try {
								resultsFeedback = searchFeedback.get().getAllResults();
							} catch (InterruptedException | ExecutionException e) {
								e.printStackTrace();
							}
							// Si la liste est non vide, cela signifie que la personne a commenté à propos de ce produit
							if(!resultsFeedback.isEmpty()) {
								// On retourne le résultat
								System.out.println("[RESULTAT] : id: " + id + "; firstName: " + firstName + "; LastName: " + lastName);
							} 
						}
					} else {
						System.out.println("[INFO] - aucune personne n'a acheté le produit: " + asin);
					}
				}
			} else {
				System.out.println("[INFO] - aucun produit trouvé pour l'asin: " + asin);
			}
		} else {
			System.out.println("[ERROR] - format de la période entrée invalide, fin du traitement");
		}

	}
	
	/**
	 * Query 8
	 * @param category catégorie du vendeur (ex: "clothing", "ice_hockey", "sportswear", etc...)
	 * @param year l'année à étudier
	 */
	public void getTotalSalesAndPopularityOfProduct(String category, String year) {
		
		JSONParser parser = new JSONParser();
		
		try {
			
			int date = Integer.parseInt(year);
			System.out.println(date);
			String key = "";
			//Les totaux des ventes
			int totalVente = 0;
			// Notre liste des produits concernés
			List<Map<String, List<String>>> resultsInvoiceToInclude = new ArrayList<Map<String,List<String>>>();
			
			
			// Tout d'abord, on recherche parmis tout les vendeurs leur catégorie (ex: dans "vendor" il est renseigné dans les parenthèses)
			SearchOperation searchVendor = new SearchOperation.Builder(BinaryValue.create("searchVendor"), "vendor:*" + category + "*").build();
			cluster.execute(searchVendor);
			List<Map<String, List<String>>> resultsVendor = new ArrayList<Map<String,List<String>>>();
			try {
				resultsVendor = searchVendor.get().getAllResults();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
			
			// On récupère les clés primaires obtenues
			Map<String, List<String>> doc = resultsVendor.get(0);
			if(!resultsVendor.isEmpty()) {
				for(Map<String, List<String>> res : resultsVendor) {
					// On récupère les clés de chaque vendeur
					key = res.get("_yz_rk").get(0);
					// Avec ces clés, on fait la jointure avec Product.brand
					SearchOperation searchProduct = new SearchOperation.Builder(BinaryValue.create("searchProduct"), "brand:" + key).build();
					cluster.execute(searchProduct);
					List<Map<String, List<String>>> resultsProduct = new ArrayList<Map<String,List<String>>>();
					try {
						resultsProduct = searchProduct.get().getAllResults();
					} catch (InterruptedException | ExecutionException e) {
						e.printStackTrace();
					}
					if(!resultsProduct.isEmpty()) {
						for(Map<String, List<String>> res1 : resultsProduct) {
							// On récupère les clés de chaque produit
							key = res1.get("_yz_rk").get(0);
							// Avec ces clés, on fait la jointure avec Invoice.asin
							SearchOperation searchInvoice = new SearchOperation.Builder(BinaryValue.create("searchInvoice"), "orderLine:*asin:" + key + "*").build();
							cluster.execute(searchInvoice);
							List<Map<String, List<String>>> resultsInvoice = new ArrayList<Map<String,List<String>>>();
							try {
								resultsInvoice = searchInvoice.get().getAllResults();
							} catch (InterruptedException | ExecutionException e) {
								e.printStackTrace();
							}
							if(!resultsInvoice.isEmpty()) {
								
								for(Map<String, List<String>> res2 : resultsInvoice) {
									// On récupère l'année
									int yearOfInvoice = Integer.parseInt(res2.get("orderDate").get(0));
									// Si elle est incluse dans la période que l'on souhaite étudier
									if(date == yearOfInvoice) {
										resultsInvoiceToInclude.add(res2);
									}
								}
								// De notre liste affiné, on récupère les totaux des prix
								// On défini notre compteur
								for(Map<String, List<String>> list : resultsInvoiceToInclude) {
									
									String toCast = list.get("orderLine").get(0);
									JSONObject json = new JSONObject();
									try {
										json = (JSONObject) parser.parse(toCast);
										totalVente += Integer.parseInt(json.get("asin").toString());
									} catch (org.json.simple.parser.ParseException e) {
										System.out.println("[WARN] - erreur format json > input: " + toCast);
									}

								}
								
							} else {
								System.out.println("[INFO] - aucune facture trouvé pour la catégorie: " + category);
							}
						}
					} else {
						System.out.println("[INFO] - aucun produit trouvé pour la catégorie: " + category);
					}
				}
				//Après avoir récupéré tout les produits vendus (totaux) pour l'année étudiée et la catégorie spécifiée, on recherche les commentaires associés aux ventes
				System.out.println("[TOTAUX DES VENTES : CATEGORIE -> "+ category +"] = " + totalVente);
				System.out.println("[FEEDBACK] -> \n --- BEGIN ---");
				for(Map<String, List<String>> list : resultsInvoiceToInclude) {
					// On récupère les produits liés aux factures
					String toCast = list.get("orderLine").get(0);
					JSONObject json = new JSONObject();
					try {
						json = (JSONObject) parser.parse(toCast);
						String asin = json.get("asin").toString();
						// On récupère les feedback en fonction des asin récupérés
						SearchOperation searchFeedback = new SearchOperation.Builder(BinaryValue.create("searchFeedback"), "asin:" + asin).build();
						cluster.execute(searchFeedback);
						List<Map<String, List<String>>> resultsFeedback = new ArrayList<Map<String,List<String>>>();
						try {
							resultsFeedback = searchFeedback.get().getAllResults();
						} catch (InterruptedException | ExecutionException e) {
							e.printStackTrace();
						}
						if(!resultsFeedback.isEmpty()) {
							for(Map<String, List<String>> l : resultsFeedback) {
								String feedback = l.get("feedback").get(0);
								System.out.println(asin + " > " + feedback);
							}
							// Fin du programme
							System.out.println(" --- END ---");
						} else {
							System.out.println("> Aucun feedback trouvé concernant les produits trouvés \n --- END ---");
						}
					} catch (org.json.simple.parser.ParseException e) {
						System.out.println("[WARN] - erreur format json > input: " + toCast);
					}
				}
				
			} else {
				System.out.println("[INFO] - aucun vendeur trouvé pour la catégorie: " + category);
			}
			
		} catch(NumberFormatException e) {
			System.out.println("[WARN]  - Date invalide, arret du traitement");
		}
	}
	
}
