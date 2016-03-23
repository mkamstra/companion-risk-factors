package no.stcorp.com.companion.traffic;

import java.util.List;
import java.util.ArrayList;

public class MeasurementSiteSegment extends MeasurementSite {
	private List<Location> mCoordinates = new ArrayList<Location>();

	public List<Location> getCoordinates() {
		return mCoordinates;
	}

	public void addCoordinate(float pLatitude, float pLongitude) {
		Location loc = new Location(pLatitude, pLongitude, 0.0);
		mCoordinates.add(loc);
	}
}