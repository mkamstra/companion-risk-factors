package no.stcorp.com.companion.visualization;

import java.io.*;
import java.nio.file.*;

import java.time.*;
import java.util.Date;

import org.jfree.data.time.*;

/**
 * Class holding the data that for example can be plotted
 */
public class TimeSeriesDataContainer {
	public enum SeriesType {
	  TRAFFICSPEED, 
	  TEMPERATURE, 
	  PRECIPITATION, 
	  WINDSPEED;
	}

	TimeSeries temperatureSeries = new TimeSeries("Temperature (C)");
	TimeSeries precipitationSeries = new TimeSeries("Precipitation (mm/h)");
	TimeSeries windspeedSeries = new TimeSeries("Wind Speed (m/s)");
	TimeSeries trafficspeedSeries = new TimeSeries("Traffic speed (km/h)");

	/**
	 * Read data from file for a specific time series
	 */
    public void importDataSeries(SeriesType pSeriesType, String pFileName) {
    	try {
	    	Path path = Paths.get(pFileName);
	    	byte[] byteArray = Files.readAllBytes(path);
	    	ObjectInput in = new ObjectInputStream(new ByteArrayInputStream(byteArray));
    		switch (pSeriesType) {
    			case TRAFFICSPEED:
    				trafficspeedSeries = (TimeSeries) in.readObject();
    				break;
    			case TEMPERATURE:
    				temperatureSeries = (TimeSeries) in.readObject();
    				break;
    			case PRECIPITATION:
    				precipitationSeries = (TimeSeries) in.readObject();
    				break;
    			case WINDSPEED:
    				windspeedSeries = (TimeSeries) in.readObject();
    				break;
    			default:
    				break;
	    	}
	    	in.close();
	    } catch (Exception ex) {
	    	ex.printStackTrace();
	    }
    }


    /**
     * Write data to file (byte arrays)
     */
    public void writeDataToFile(String pNdwId, String pStartDate, String pEndDate) {
    	writeDataToFile(SeriesType.TRAFFICSPEED, pNdwId, pStartDate, pEndDate);
    	writeDataToFile(SeriesType.TEMPERATURE, pNdwId, pStartDate, pEndDate);
    	writeDataToFile(SeriesType.PRECIPITATION, pNdwId, pStartDate, pEndDate);
    	writeDataToFile(SeriesType.WINDSPEED, pNdwId, pStartDate, pEndDate);
    }

    private void writeDataToFile(SeriesType pSeriesType, String pNdwId, String pStartDate, String pEndDate) {
		OutputStream fileOutput = null;
		ByteArrayOutputStream baos = null;
		ObjectOutput out = null;
    	try {
    		String fileName = "/tmp/" + pSeriesType + "_" + pNdwId + "_" + pStartDate + "_" + pEndDate + ".bos";
    		System.out.println("Writing chart data to file: " + fileName);
    		fileOutput = new FileOutputStream(fileName);
    		baos = new ByteArrayOutputStream();
    		out = new ObjectOutputStream(baos);
    		switch (pSeriesType) {
    			case TRAFFICSPEED:
    				out.writeObject(trafficspeedSeries);
    				break;
    			case TEMPERATURE:
    				out.writeObject(temperatureSeries);
    				break;
    			case PRECIPITATION:
    				out.writeObject(precipitationSeries);
    				break;
    			case WINDSPEED:
    				out.writeObject(windspeedSeries);
    				break;
    			default:
    				break;
    		}
    		baos.writeTo(fileOutput);
    	} catch (Exception ex) {
    		ex.printStackTrace();
    	} finally {
    		try {
	    		out.close();
	    		baos.close();
	    		fileOutput.close();
	    	} catch (Exception ex) {
	    		System.err.println("Error closing output");
	    		ex.printStackTrace();
	    	}
    	}
    }

    public void addTemperatureRecord(Instant pTime, double pTemperature) {
    	Hour hour = new Hour(Date.from(pTime));
    	if (temperatureSeries.getDataItem(hour) != null) {
    		System.err.println("Trying to add temperature at hour " + hour + " but a value for that hour exists already");
    	} else {
    		temperatureSeries.add(hour, pTemperature);
    	}
    }

    public void addPrecipitationRecord(Instant pTime, double pPrecipitation) {
    	Hour hour = new Hour(Date.from(pTime));
    	if (precipitationSeries.getDataItem(hour) != null) {
    		System.err.println("Trying to add precipitation at hour " + hour + " but a value for that hour exists already");
    	} else {
    		precipitationSeries.add(hour, pPrecipitation);
    	}
    }

    public void addWindspeedRecord(Instant pTime, double pWindspeed) {
    	Hour hour = new Hour(Date.from(pTime));
    	if (windspeedSeries.getDataItem(hour) != null) {
    		System.err.println("Trying to add wind speed at hour " + hour + " but a value for that hour exists already");
    	} else {
    		windspeedSeries.add(hour, pWindspeed);
    	}
    }

    public void addTrafficspeedRecord(Instant pTime, double pTrafficspeed) {
    	Minute minute = new Minute(Date.from(pTime));
    	if (trafficspeedSeries.getDataItem(minute) != null) {
    		System.err.println("Trying to add traffic speed at minute " + minute + " but a value for that minute exists already");
    	} else {
    		trafficspeedSeries.add(minute, pTrafficspeed);
    	}
    }
  
  	public TimeSeries getTrafficspeedSeries() {
  		return trafficspeedSeries;
  	}

  	public TimeSeries getTemperatureSeries() {
  		return temperatureSeries;
  	}

  	public TimeSeries getPrecipitationSeries() {
  		return precipitationSeries;
  	}

  	public TimeSeries getWindspeedSeries() {
  		return windspeedSeries;
  	}
}