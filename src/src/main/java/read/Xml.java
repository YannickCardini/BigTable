package src.main.java.read;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import src.main.java.documents.Doc;
import src.main.java.documents.Order;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class Xml {

	private ArrayList<Order> invoices = new ArrayList<Order>();
	private ArrayList<Doc> docs = new ArrayList<Doc>();


	public ArrayList<Doc> getDocs() {
		return docs;
	}

	public void setDocs(ArrayList<Doc> docs) {
		this.docs = docs;
	}

	public ArrayList<Order> getInvoices() {
		return invoices;
	}

	public void setInvoices(ArrayList<Order> invoices) {
		this.invoices = invoices;
	}

	public void readInvoice(String pathToCsv) {
		try {

			File file = new File(pathToCsv);
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(file); // demande enormement de mémoire car le fichier est mega lourd (comme félin)
			doc.getDocumentElement().normalize();
			NodeList nodeList = doc.getElementsByTagName("Invoice.xml");
			SimpleDateFormat formatOrder = new SimpleDateFormat("yyyy-MM-dd");
//			Element eElement = null;
			Node node = null;
			NodeList orderLine = null;

			for (int itr = 0; itr < nodeList.getLength(); itr++) {
				node = nodeList.item(itr);
				if (node.getNodeType() == Node.ELEMENT_NODE) {
					Element eElement = (Element) node;
					Order inv = new Order();

					String orderId = eElement.getElementsByTagName("OrderId").item(0).getTextContent();
					inv.setOrderId(orderId);

					String personId = eElement.getElementsByTagName("PersonId").item(0).getTextContent();
					inv.setPersonId(personId);

					String orderDate = eElement.getElementsByTagName("OrderDate").item(0).getTextContent();
					inv.setOrderDate((Date) formatOrder.parse(orderDate));

					String totalPrice = eElement.getElementsByTagName("TotalPrice").item(0).getTextContent();
					inv.setTotalPrice(Double.parseDouble(totalPrice));

					orderLine = eElement.getElementsByTagName("Orderline");
					

					for (int j = 0; j < orderLine.getLength(); j++) {
						Node order = orderLine.item(j);
						Doc docu = new Doc();

						if (order.getNodeType() == Node.ELEMENT_NODE) {
							Element e = (Element) order;

							String productId = e.getElementsByTagName("productId").item(0).getTextContent();
							docu.setProductId(productId);

							String asin = e.getElementsByTagName("asin").item(0).getTextContent();
							docu.setAsin(asin);

							String title = e.getElementsByTagName("title").item(0).getTextContent();
							docu.setTitle(title);

							String price = e.getElementsByTagName("price").item(0).getTextContent();
							docu.setPrice(Double.parseDouble(price));

							String brand = e.getElementsByTagName("brand").item(0).getTextContent();
							docu.setBrand(brand);
						}
						docs.add(docu);

					}
					inv.setOrderline(getDocs());
					invoices.add(inv);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}