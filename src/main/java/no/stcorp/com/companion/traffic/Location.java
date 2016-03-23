package no.stcorp.com.companion.traffic;

public class Location {
	private double mLatitude;
	private double mLongitude;
	private double mAltitude;

	public Location(double pLatitude, double pLongitude, double pAltitude) {
		mLatitude = pLatitude;
		mLongitude = pLongitude;
		mAltitude = pAltitude;
	}

	public double getLatitude() {
		return mLatitude;
	}

	public double getLongitude() {
		return mLongitude;
	}

	public double getAltitude() {
		return mAltitude;
	}

	public String getLatLonForPostGis() {
		return "" + mLatitude + " " + mLongitude;
	}
}