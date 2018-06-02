package kafka.avro.schema;

public class User {

	private String userName;
	private int favoriteNumber;
	private String FavoriteColor;
	
	
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public int getFavoriteNumber() {
		return favoriteNumber;
	}
	public void setFavoriteNumber(int favoriteNumber) {
		this.favoriteNumber = favoriteNumber;
	}
	public String getFavoriteColor() {
		return FavoriteColor;
	}
	public void setFavoriteColor(String favoriteColor) {
		FavoriteColor = favoriteColor;
	}
	
}
