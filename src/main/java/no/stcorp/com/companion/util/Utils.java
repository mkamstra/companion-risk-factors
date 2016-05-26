package no.stcorp.com.companion.util;

import org.jfree.data.time.*;

import java.io.*;

import java.nio.file.*;

import java.time.Instant;

import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

/**
 * A container for some generally applicable methods
 */
public class Utils {

    public static void printFileDetailsForFolder(Path pFolder) {
        System.out.println("============== Files in folder: " + pFolder.toString() + " ================");
        try {
            Files.walk(pFolder).forEach(filePath -> {
                if (Files.isRegularFile(filePath)) {
                    try {
                        System.out.println(filePath + " (" + Files.size(filePath) + ")");
                    } catch (AccessDeniedException e) {
                        System.out.println("Problem accessing file " + filePath);
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        } catch (AccessDeniedException ex) {
            System.out.println("Problem accessing file");
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void waitForUserInput() {
        Scanner scan = new Scanner(System.in);
        System.out.println("Continue by pressing any key followed by ENTER");
        try {
            scan.nextInt();
        } catch (Exception ex) {
            // Not important to write anything
        }
    }

    /**
     * Convert a JFreeChart RegularTimePeriod (which represents data points in JFreeChart TimeSeries) to
     * Java Instant objects (start and end)
     */
    public static double[] convertTimeSeriesDataItemToDouble(TimeSeriesDataItem tsdp) {

        RegularTimePeriod rtp = tsdp.getPeriod();
        Date start = rtp.getStart();
        Date end = rtp.getEnd();

        double startEpoch = start.toInstant().getEpochSecond();
        double stopEpoch = end.toInstant().getEpochSecond();
        double value = tsdp.getValue().doubleValue();

        return new double[]{startEpoch, stopEpoch, value};
    }

    public static List<double[]> convertTimeSeriesToList(TimeSeries ts) {

        int count = ts.getItemCount();

        List<TimeSeriesDataItem> items = ts.getItems();
        List<double[]> ds = items.stream().map(tsp -> convertTimeSeriesDataItemToDouble(tsp)).collect(Collectors.toList());

        return ds;
    }

    private static final long MEGABYTE = 1024L * 1024L;

    public static long bytesToMegabytes(long bytes) {
        return bytes / MEGABYTE;
    }

    public static String getMemoryUsage() {
        Runtime runtime = Runtime.getRuntime();
        String memoryString = "Memory usage: total memory = " + Utils.bytesToMegabytes(runtime.totalMemory()) +
                " MB, current usage = " + Utils.bytesToMegabytes(runtime.totalMemory() - runtime.freeMemory()) +
                " MB, free memory = " + Utils.bytesToMegabytes(runtime.freeMemory());

        return memoryString;
    }

}