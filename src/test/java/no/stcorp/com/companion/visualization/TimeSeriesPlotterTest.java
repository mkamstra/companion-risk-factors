package no.stcorp.com.companion.visualization;

import static org.junit.Assert.*;

import org.junit.Test;

import java.time.*;
import java.time.format.*;

/**
 * Class for testing the TimeSeriesPlotter
 */
public class TimeSeriesPlotterTest {

	@Test
  public void testWriteDataAndPlot() {
    System.out.println("Test writing data to file");
    String ndwId = "TestSiteNDW";
    String timeStartString = "20160110";
    String timeEndString = "20160119";
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHH").withZone(ZoneId.systemDefault());
    DateTimeFormatter formatterComplete = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
    TimeSeriesDataContainer tdc = new TimeSeriesDataContainer();
    for (int day = 10; day <= 19; day++) {
      for (int hour = 0; hour <= 23; hour++) {
        double temperature = 8.0 + 2.0 * Math.random();
        double precipitation = Math.max(0.0, (Math.random() - 0.9) * 100.0);
        double windspeed = 3.0 + (Math.random() - 0.5) * 4.0;
        Instant timeObservation = formatter.parse("201601" + String.format("%02d", day) + String.format("%02d", hour), ZonedDateTime::from).toInstant();
        tdc.addTemperatureRecord(timeObservation, temperature);
        tdc.addPrecipitationRecord(timeObservation, precipitation);
        tdc.addWindspeedRecord(timeObservation, windspeed);
        for (int minute = 0; minute < 60; minute += 20) {
          String timeTrafficString = "2016-01-" + String.format("%02d", day) + " " + String.format("%02d", hour) + ":" + String.format("%02d", minute) + ":" + "00";
          Instant timeTraffic = formatterComplete.parse(timeTrafficString, ZonedDateTime::from).toInstant();
          double averageSpeed = 70.0 + (Math.random() - 0.5) * 40.0;
          tdc.addTrafficspeedRecord(timeTraffic, averageSpeed);
        }
      }
    }
    assertEquals(720, tdc.getTrafficspeedSeries().getItemCount());
    assertEquals(240, tdc.getTemperatureSeries().getItemCount());
    assertEquals(240, tdc.getPrecipitationSeries().getItemCount());
    assertEquals(240, tdc.getWindspeedSeries().getItemCount());
    tdc.writeDataToFile(ndwId, timeStartString, timeEndString); 
    TimeSeriesPlotter tsp = new TimeSeriesPlotter("Weather and traffic at measurement site " + ndwId);
    tsp.plot(ndwId, timeStartString, timeEndString, tdc);
  }

  @Test
  public void testReadDataAndPlot() {
    System.out.println("Test reading data from file");
    System.out.println("Working directory = " + System.getProperty("user.dir"));
    String dataFilesPath = "src/test/resources/chartdata/";
    String fileNameTrafficspeed = dataFilesPath + "TRAFFICSPEED_RWS01_MONIBAS_0131hrl0035ra_2015-12-01-00_2015-12-01-24.bos";
    String fileNameTemperature = dataFilesPath + "TEMPERATURE_RWS01_MONIBAS_0131hrl0035ra_2015-12-01-00_2015-12-01-24.bos";
    String fileNamePrecipitation = dataFilesPath + "PRECIPITATION_RWS01_MONIBAS_0131hrl0035ra_2015-12-01-00_2015-12-01-24.bos";
    String fileNameWindspeed = dataFilesPath + "WINDSPEED_RWS01_MONIBAS_0131hrl0035ra_2015-12-01-00_2015-12-01-24.bos";
    String ndwId = "TestSiteNDW";
    String timeStartString = "20160110";
    String timeEndString = "20160119";
    TimeSeriesDataContainer tdc = new TimeSeriesDataContainer();
    tdc.importDataSeries(TimeSeriesDataContainer.SeriesType.TRAFFICSPEED, fileNameTrafficspeed);
    tdc.importDataSeries(TimeSeriesDataContainer.SeriesType.TEMPERATURE, fileNameTemperature);
    tdc.importDataSeries(TimeSeriesDataContainer.SeriesType.PRECIPITATION, fileNamePrecipitation);
    tdc.importDataSeries(TimeSeriesDataContainer.SeriesType.WINDSPEED, fileNameWindspeed);
    assertEquals(137, tdc.getTrafficspeedSeries().getItemCount());
    assertEquals(24, tdc.getTemperatureSeries().getItemCount());
    assertEquals(24, tdc.getPrecipitationSeries().getItemCount());
    assertEquals(24, tdc.getWindspeedSeries().getItemCount());
    TimeSeriesPlotter tsp = new TimeSeriesPlotter("Weather and traffic at measurement site " + ndwId);
    tsp.plot(ndwId, timeStartString, timeEndString, tdc);
  }

}