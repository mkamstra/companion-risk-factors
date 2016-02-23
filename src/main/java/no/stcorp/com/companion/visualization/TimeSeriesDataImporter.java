package no.stcorp.com.companion.visualization;

import org.apache.commons.io.*;

import java.io.*;

import java.time.*;
import java.time.format.*;

import java.util.*;
import java.util.Map.*;

public class TimeSeriesDataImporter { 
  private Map<String, Instant> mStartTimePerNdw = new HashMap<String, Instant>();
  private Map<String, Instant> mEndTimePerNdw = new HashMap<String, Instant>();

  public Map<String, TimeSeriesDataContainer> importTimeSeriesDataFromFiles() {
    TimeSeriesFileSelector tsf = new TimeSeriesFileSelector();
    File[] selectedFiles = tsf.selectFiles();
    Map<String, TimeSeriesDataContainer> tdcPerNdw = new HashMap<String, TimeSeriesDataContainer>();
    TimeSeriesDataContainer.SeriesType seriesType = null;
    DateTimeFormatter fileNameFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH").withZone(ZoneId.systemDefault());
    for (File file : selectedFiles) {
      try {
        String fileName = file.getName();
        if (fileName.toLowerCase().startsWith("trafficspeed")) {
          seriesType = TimeSeriesDataContainer.SeriesType.TRAFFICSPEED;
        } else if (fileName.toLowerCase().startsWith("temperature")) {
          seriesType = TimeSeriesDataContainer.SeriesType.TEMPERATURE;
        } else if (fileName.toLowerCase().startsWith("precipitation")) {
          seriesType = TimeSeriesDataContainer.SeriesType.PRECIPITATION;
        } else if (fileName.toLowerCase().startsWith("windspeed")) {
          seriesType = TimeSeriesDataContainer.SeriesType.WINDSPEED;
        }

        // Get date and ndw id from file name
        String baseName = FilenameUtils.getBaseName(fileName);
        System.out.println("Base name: " + baseName);
        int startOfEndDate = baseName.lastIndexOf("_") + 1;
        String fileEndDateString = baseName.substring(startOfEndDate);
        System.out.println("End date: " + fileEndDateString);
        String remainingBaseName = baseName.substring(0, startOfEndDate - 1);
        int startOfStartDate = remainingBaseName.lastIndexOf("_") + 1;
        String fileStartDateString = remainingBaseName.substring(startOfStartDate);
        System.out.println("Start date: " + fileStartDateString);
        String baseNameWithoutDates = remainingBaseName.substring(0, startOfStartDate - 1);
        int startOfNdwId = baseNameWithoutDates.indexOf("_");
        String ndwId = baseNameWithoutDates.substring(startOfNdwId + 1);
        System.out.println("NDW id: " + ndwId);

        Instant startTime = fileNameFormatter.parse(fileStartDateString, ZonedDateTime::from).toInstant();
        Instant endTime = fileNameFormatter.parse(fileEndDateString, ZonedDateTime::from).toInstant();

        TimeSeriesDataContainer tdc = null;

        if (tdcPerNdw.containsKey(ndwId)) {
          tdc = tdcPerNdw.get(ndwId);
        } else  {
          tdc = new TimeSeriesDataContainer(); 
          tdcPerNdw.put(ndwId, tdc);
        }
        System.out.println("TDC: " + tdc);

        if (mStartTimePerNdw.containsKey(ndwId)) {
          Instant existingStartTime = mStartTimePerNdw.get(ndwId);
          if (startTime.isBefore(existingStartTime)) 
            mStartTimePerNdw.put(ndwId, startTime);
        } else {
          mStartTimePerNdw.put(ndwId, startTime);
        }

        if (mEndTimePerNdw.containsKey(ndwId)) {
          Instant existingEndTime = mEndTimePerNdw.get(ndwId);
          if (endTime.isAfter(existingEndTime)) 
            mEndTimePerNdw.put(ndwId, endTime);
        } else {
          mEndTimePerNdw.put(ndwId, endTime);
        }

        tdc.importDataSeries(seriesType, file.getPath());
      } catch (Exception ex) {
        System.out.println("Something went wrong reading plotting data from the selected files: " + ex.getMessage());
        ex.printStackTrace();
      }
    }

    return tdcPerNdw;
  }

  public Map<String, Instant> getStartTimePerNdw() {
    return mStartTimePerNdw;
  }

  public Map<String, Instant> getEndTimePerNdw() {
  	return mEndTimePerNdw;
  }

}