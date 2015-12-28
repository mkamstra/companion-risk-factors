public class WeatherStation {
	private int knmiId;
	private String name;
	private float latitude;
	private float longitude;
	private float altitude;

	public WeatherStation(int knmiId, String name, float latitude, float longitude, float altitude) {
		this.knmiId = knmiId;
		this.name = name;
		this.latitude = latitude;
		this.longitude = longitude;
		this.altitude = altitude;
	}

	public int getKnmiId() {
		return knmiId;
	}

	public String getName() {
		return name;
	}

	public float getLatitude() {
		return latitude;
	}

	public float getLongitude() {
		return longitude;
	}

	public float getAltitude() {
		return altitude;
	}
}