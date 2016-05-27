package no.stcorp.com.companion.aggregate;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import no.stcorp.com.companion.traffic.*;
import no.stcorp.com.companion.util.Utils;
import no.stcorp.com.companion.visualization.*;

import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.*;

import java.util.*;
import java.util.Map.*;

/**
 * Class responsible for aggregating traffic and weather for measurement sites
 */
public class TrafficWeatherAggregator {

    public enum ExportFormat {
        NOEXPORT,
        BOS,
        HDF5
    }

    private IHDF5Writer writer;

    /**
     * Get the speed measurements and weather observations per measurement site. They are also stored as plot data and eventually plotted
     *
     * @param pCurrentSpeedMeasurementsForMeasurementsSites A map of speed measurements per measurement site (key: NDW id)
     * @param pWeatherObservationsForMeasurementSites       A map of weather observations per measurement site (key: NDW id)
     * @param pStartDateString                              Start date in format yyyyMMddHH
     * @param pEndDateString                                End date in format yyyyMMddHH
     * @param pPlot                                         Flag indicating whether plots need to be generated or not
     * @param pExportFormat                                 The format to export (None, HDF5 and BOS)
     * @param pExportPath                                   The folder to export to
     * @param pFileStartIndex                               The starting index of the traffic speed files to be used (needed for export naming to avoid overriding existing files)
     * @param pFileEndIndex                                 The ending index (exclusive) of the traffic speed files to be used (needed for export naming to avoid overriding existing files)
     * @param pGenerationDate                               The date this export is running
     */
    public void exportWeatherAndTrafficPerMeasurementSite(Map<String, List<SiteMeasurement>> pCurrentSpeedMeasurementsForMeasurementsSites,
                                                          Map<String, List<String>> pWeatherObservationsForMeasurementSites, String pStartDateString, String pEndDateString,
                                                          boolean pPlot, ExportFormat pExportFormat, String pExportPath, int pFileStartIndex, int pFileEndIndex, Instant pGenerationDate) {

        if (pExportFormat == ExportFormat.HDF5) {
            DateTimeFormatter fileNameDateFormat = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneId.systemDefault()); // the format for the part of the file name
            String fileName = pExportPath + "TS_" + pStartDateString + "_" + pEndDateString + "_" + pFileStartIndex + "-" + pFileEndIndex + "_" + fileNameDateFormat.format(pGenerationDate) + ".hdf";
            Date date = new Date(System.currentTimeMillis());
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH"); // the format for the attribute within the file
            System.out.println("Writing weather and traffic data for selected measurement points at selected time interval: " + fileName);
            writer = HDF5Factory.open(fileName);
            writer.string().setAttr("/", "generation_datetime", sdf.format(date));
            writer.string().setAttr("/", "start_datetime", pStartDateString);
            writer.string().setAttr("/", "end_datetime", pEndDateString);
        }

        // Loop over the speed measurements per measurement site
        int counter = 0;
        long startTime = System.currentTimeMillis();
        int numberOfMPs = pCurrentSpeedMeasurementsForMeasurementsSites.size();
        for (Entry<String, List<SiteMeasurement>> speedEntry : pCurrentSpeedMeasurementsForMeasurementsSites.entrySet()) {
            counter++;
            if (counter % 1000 == 0) {
                long currentTime = System.currentTimeMillis();
                double usedTime = (currentTime - startTime) / 1000;
                System.out.println("Linking traffic with weather and writing to file: [" + counter + " out of " + numberOfMPs + "]  Time used: " + usedTime + " [s]");
                System.out.println(Utils.getMemoryUsage());
            }
            String ndwId = speedEntry.getKey();
            List<SiteMeasurement> sms = speedEntry.getValue();
            //System.err.println("=================================================");
            //System.out.println("Measurement site: " + ndwId);
            // Check if there are weather observations for the current measurement site
            if (pWeatherObservationsForMeasurementSites.containsKey(ndwId)) {
                List<String> wos = pWeatherObservationsForMeasurementSites.get(ndwId);
                // Weather is available on an hourly basis, whereas traffic is recorded on a higher frequency (usually every minute). The number of the hour
                // in the weather represents the weather of the previous hour, i.e. 15 represents 14-15. A line of weather typically contains:
                // # STN,YYYYMMDD,   HH,   DD,   FH,   FF,   FX,    T,  T10,   TD,   SQ,    Q,   DR,   RH,    P,   VV,    N,    U,   WW,   IX,    M,    R,    S,    O,    Y
                // Loop over the weather observations which are sorted in time
                // Create a plot for each day
                TimeSeriesDataContainer tdc = new TimeSeriesDataContainer();
                for (String wo : wos) {
                    storeTrafficAndWeatherDataForOneMeasurementSite(wo, tdc, sms);
                }
                // Finished, so show the plot
                if (pPlot) {
                    //System.out.println("Plotting due to end of loop");
                    TimeSeriesPlotter tsp = new TimeSeriesPlotter("Weather and traffic at measurement site " + ndwId);
                    tsp.plot(ndwId, pStartDateString, pEndDateString, tdc);
                }
                switch (pExportFormat) {
                    case NOEXPORT:
                        break;
                    case BOS:
                        tdc.writeDataToFile(pExportPath, ndwId, pStartDateString, pEndDateString);
                        break;
                    case HDF5:
                        tdc.writeHDF5(writer, ndwId);
                        break;
                }
            } else {
                System.err.println("  No weather observations for measurement site: " + ndwId);
            }
            //System.err.println("-------------------------------------------------");
        }

        if (pExportFormat == ExportFormat.HDF5) {
            writer.close();
        }

        System.out.println(Utils.getMemoryUsage());
    }

    /**
     * @param pWeatherObservationLine   A string containing one weather observation (one line in a weather observation file for one specific time and one location)
     * @param pTimeSeriesDataContainer  A data container for the measurement site which contains the time series that will be used to save the weather and traffic data to
     * @param pTrafficSpeedMeasurements A list of traffic speed measurements for a specific measurement site
     *                                  <p>
     *                                  Store the traffic and weather data of one measurement site
     */
    private void storeTrafficAndWeatherDataForOneMeasurementSite(String pWeatherObservationLine, TimeSeriesDataContainer pTimeSeriesDataContainer, List<SiteMeasurement> pTrafficSpeedMeasurements) {
        try {
            String[] woElements = pWeatherObservationLine.split(",");
            if (woElements.length == 25) {
                String dateString = woElements[1].trim();
                String hourString = woElements[2].trim();
                int hours = Integer.valueOf(hourString);
                String windspeedString = woElements[5].trim(); // Wind speed in 0.1 m/s
                int windspeed01 = 0;
                if (windspeedString.length() > 0) {
                    windspeed01 = Integer.valueOf(windspeedString);
                }

                double windspeed = windspeed01 / 10.0;
                String temperatureString = woElements[7].trim(); // Temperature in 0.1 Celsius
                if (temperatureString.length() == 0) {
                    return; // We need temperature
                }

                int temperature01 = Integer.valueOf(temperatureString);
                double temperature = temperature01 / 10.0;
                String precipitationString = woElements[13].trim(); // Precipitation in 0.1 mm/h
                int precipitation01 = 0;
                if (precipitationString.length() > 0) {
                    precipitation01 = Integer.valueOf(precipitationString);
                }
                double precipitation = Math.max(0.0, precipitation01 / 10.0); // Can be negative: -1 means < 0.05 mm/h, but we ignore that for now

                String timeEndString = dateString + String.format("%02d", hours);
                String timeStartString = dateString + String.format("%02d", hours - 1);
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHH").withZone(ZoneId.systemDefault());
                Instant timeStart = formatter.parse(timeStartString, ZonedDateTime::from).toInstant();
                Instant timeEnd = formatter.parse(timeEndString, ZonedDateTime::from).toInstant();
                pTimeSeriesDataContainer.addTemperatureRecord(timeEnd, temperature);
                pTimeSeriesDataContainer.addPrecipitationRecord(timeEnd, precipitation);
                pTimeSeriesDataContainer.addWindspeedRecord(timeEnd, windspeed);
                //System.out.println("Just added for hour " + timeEndString + " : temperature = " + temperature + ", precipitation = " + precipitation + ", windspeed = " + windspeed);
                //waitForUserInput();
                //System.out.println("    --------- Weather observation and traffic measurements for the same hour -----------" + pWeatherObservationLine);
                //System.out.println("    " + pWeatherObservationLine);
                DateTimeFormatter formatterComplete = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
                try {
                    int hour = Integer.valueOf(hourString.trim());
                    List<SiteMeasurement> relevantSms = new ArrayList<SiteMeasurement>();
                    for (SiteMeasurement sm : pTrafficSpeedMeasurements) {
                        Instant timeSm = sm.getMeasurementTimeDefault();
                        if ((timeSm.isAfter(timeStart) && timeSm.isBefore(timeEnd)) || timeSm.equals(timeStart) || timeSm.equals(timeEnd)) {
                            // This traffic measurement is in the same hour as the weather observation
                            //System.out.println("      " + sm);
                            List<MeasuredValue> mvs = sm.getMeasuredValues();
                            double sumFlow = 0;
                            double sumSpeed = 0;
                            int nrOfFlows = 0;
                            int nrOfSpeeds = 0;
                            for (MeasuredValue mv : mvs) {
                                String type = mv.getType();
                                if (type.equalsIgnoreCase("TrafficSpeed")) {
                                    double speed = mv.getValue();
                                    if (speed < 0)
                                        continue;
                                    else {
                                        sumSpeed += speed;
                                        nrOfSpeeds++;
                                    }
                                } else if (type.equalsIgnoreCase("TrafficFlow")) {
                                    double flow = mv.getValue();
                                    if (flow < 0)
                                        continue;
                                    else {
                                        sumFlow += flow;
                                        nrOfFlows++;
                                    }
                                }
                            }
                            double averageSpeed = 0.0;
                            if (nrOfSpeeds > 0)
                                averageSpeed = sumSpeed / (double) nrOfSpeeds;

                            double averageFlow = 0.0;
                            if (nrOfFlows > 0)
                                averageFlow = sumFlow / (double) nrOfFlows;

                            //System.out.println("Adding speed/flow record: " + formatterComplete.format(timeSm) + ", speed: " + averageSpeed + ", flow:" + averageFlow);
                            //waitForUserInput();

                            pTimeSeriesDataContainer.addTrafficflowRecord(timeSm, averageFlow);
                            pTimeSeriesDataContainer.addTrafficspeedRecord(timeSm, averageSpeed);
                            relevantSms.add(sm);
                        }
                    }
                } catch (Exception ex) {
                    System.err.println("Weather record hour should only contain integer values");
                    ex.printStackTrace();
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}