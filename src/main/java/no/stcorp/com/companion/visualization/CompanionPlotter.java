package no.stcorp.com.companion.visualization;

import java.time.*;
import java.time.format.*;

import java.util.*;
import java.util.Map.*;

public class CompanionPlotter {

	public void plot() {
    TimeSeriesDataImporter importer = new TimeSeriesDataImporter();
    Map<String, TimeSeriesDataContainer> tdcPerNdw = importer.importTimeSeriesDataFromFiles();
    DateTimeFormatter fileNameFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH").withZone(ZoneId.systemDefault());
    Map<String, Instant> startTimePerNdw = importer.getStartTimePerNdw();
    Map<String, Instant> endTimePerNdw = importer.getEndTimePerNdw();

    for (Entry<String, TimeSeriesDataContainer> ndwTdcEntry : tdcPerNdw.entrySet()) {
    	try {
        String ndwId = ndwTdcEntry.getKey();
        TimeSeriesDataContainer tdc = ndwTdcEntry.getValue();
        Instant startTime = startTimePerNdw.get(ndwId);
        Instant endTime = endTimePerNdw.get(ndwId);
        TimeSeriesPlotter tsp = new TimeSeriesPlotter("Weather and traffic at measurement site " + ndwId);
        tsp.plot(ndwId, fileNameFormatter.format(startTime), fileNameFormatter.format(endTime), tdc);
	    } catch (Exception ex) {
	      System.out.println("Something went wrong with plotting the selected files: " + ex.getMessage());
	      ex.printStackTrace();
	    }
    }
	}

	
}