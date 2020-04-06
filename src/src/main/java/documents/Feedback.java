package src.main.java.documents;

public class Feedback {
	
	private String asin;
	private String PersonId;
	private String feedback;
	
	public String getFeedback() {
		return feedback;
	}
	public void setFeedback(String feedback) {
		this.feedback = feedback;
	}
	public String getPersonId() {
		return PersonId;
	}
	public void setPersonId(String personId) {
		this.PersonId = personId;
	}
	public String getAsin() {
		return asin;
	}
	public void setAsin(String asin) {
		this.asin = asin;
	}

}
