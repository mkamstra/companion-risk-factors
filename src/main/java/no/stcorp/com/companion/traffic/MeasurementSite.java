package no.stcorp.com.companion.traffic;

import java.util.List;
import java.util.ArrayList;

public class MeasurementSite {
	private String ndwid;
	private String name;
	private int ndwtype;
	private Location location;
	private String carriageway = null;
	private int lengthaffected = -1;
	private List<Location> coordinates = new ArrayList<Location>(); 

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

	public void setLocation(Location pLocation) {
		location = pLocation;
	}

	public Location getLocation() {
		return location;
	}

	public void setCarriageway(String pCarriageway) {
		carriageway = pCarriageway;
	}

	public String getCarriageway() {
		return carriageway;
	}

	public void setLengthaffected(int pLengthaffected) {
		lengthaffected = pLengthaffected;
	}

	public int getLengthaffected() {
		return lengthaffected;
	}

	public void addCoordinate(Location pLocation) {
		coordinates.add(pLocation);
	}

	public List<Location> getCoordinates() {
		return coordinates;
	}

	public String getCoordinatesForPostGis() {
		String coordinateString = "";
		for (Location loc : coordinates) {
			coordinateString += loc.getLatitude() + " " + loc.getLongitude() + ", ";
		}
		coordinateString = coordinateString.trim();
		coordinateString = coordinateString.substring(0, coordinateString.length() - 1); // Remove trailing comma
		return coordinateString;
	}
}