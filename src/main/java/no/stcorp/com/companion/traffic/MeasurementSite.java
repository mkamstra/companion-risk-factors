package no.stcorp.com.companion.traffic;

public class MeasurementSite {
	private String ndwid;
	private String name;
	private int ndwtype;
	private float latitude = Float.NaN;
	private float longitude= Float.NaN;
	private String location1 = "";
	private String carriageway1 = null;
	private int lengthaffected1 = -1;
	private String location2 = null;
	private String carriageway2 = null;
	private int lengthaffected2 = -1;

	public void setNdwid(String pNdwid) {
		ndwid = pNdwid;
	}

	public String getNdwid() {
		return ndwid;
	}

	public void setName(String pName) {
		name = pName;
	}

	public String getName() {
		return name;
	}

	public void setNdwtype(int pNdwtype) {
		ndwtype = pNdwtype;
	}

	public int getNdwtype() {
		return ndwtype;
	}

	public void setLatitude(float pLatitude) {
		latitude = pLatitude;
	}

	public float getLatitude() {
		return latitude;
	}

	public void setLongitude(float pLongitude) {
		longitude = pLongitude;
	}

	public float getLongitude() {
		return longitude;
	}

	public void setLocation1(String pLocation1) {
		location1 = pLocation1;
	}

	public void addCoordinateToLocation1(float pLatitude, float pLongitude) {
		String locationString = "(" + pLatitude + ", " + pLongitude + ")";
		String commaString = ", ";
		if (location1.length() == 0) {
			location1 = "()";
			commaString = "";
		}
		location1 = location1.substring(0, location1.length() - 1) + commaString + locationString + location1.substring(location1.length() - 1);
 	}

	public String getLocation1() {
		return location1;
	}

	public void setCarriageway1(String pCarriageway1) {
		carriageway1 = pCarriageway1;
	}

	public String getCarriageway1() {
		return carriageway1;
	}

	public void setLengthaffected1(int pLengthaffected1) {
		lengthaffected1 = pLengthaffected1;
	}

	public int getLengthaffected1() {
		return lengthaffected1;
	}

	public void setLocation2(String pLocation2) {
		location2 = pLocation2;
	}

	public void addCoordinateToLocation2(float pLatitude, float pLongitude) {
		String locationString = "(" + pLatitude + ", " + pLongitude + ")";
		String commaString = ", ";
		if (location2.length() == 0) {
			location2 = "()";
			commaString = "";
		}
		location2 = location2.substring(0, location2.length() - 1) + commaString + locationString + location2.substring(location2.length() - 1);
 	}

	public String getLocation2() {
		return location2;
	}

	public void setCarriageway2(String pCarriageway2) {
		carriageway2 = pCarriageway2;
	}

	public String getCarriageway2() {
		return carriageway2;
	}

	public void setLengthaffected2(int pLengthaffected2) {
		lengthaffected2 = pLengthaffected2;
	}

	public int getLengthaffected2() {
		return lengthaffected2;
	}
}