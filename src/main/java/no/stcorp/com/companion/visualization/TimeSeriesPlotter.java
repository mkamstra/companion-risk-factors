package no.stcorp.com.companion.visualization;

import java.awt.Font;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.*;

import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.annotations.XYTextAnnotation;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.DateTickMarkPosition;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.labels.StandardXYToolTipGenerator;
import org.jfree.chart.plot.DatasetRenderingOrder;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.StandardXYItemRenderer;
import org.jfree.chart.renderer.xy.XYBarRenderer;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.data.xy.IntervalXYDataset;
import org.jfree.data.xy.XYDataset;
import org.jfree.date.SerialDate;
import org.jfree.data.time.*;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

/**
 * Class for plotting time series
 */
public class TimeSeriesPlotter extends ApplicationFrame {
	private static final long serialVersionUID = 900L;

  /**
   * Constructs a new time series plotter.
   *
   * @param title  the frame title.
   */
  public TimeSeriesPlotter(final String title) {
    super(title);
  }

	public void plot(String ndwId, String startDateString, String endDateString, TimeSeriesDataContainer tdc) {
		create(ndwId, startDateString, endDateString, tdc);
		pack();
		RefineryUtilities.centerFrameOnScreen(this);
		setVisible(true);
	}

	/**
	 * Create the chart after all data has been filled
	 */
  private void create(String pNdwId, String pStartDate, String pEndDate, TimeSeriesDataContainer tdc) {
    final JFreeChart chart = createOverlaidChart(pNdwId, pStartDate, pEndDate, tdc);
    final ChartPanel panel = new ChartPanel(chart, true, true, true, true, true);
    panel.setPreferredSize(new java.awt.Dimension(800, 500));
    setContentPane(panel);
  }

  /**
   * Creates an overlaid chart.
   *
   * @return The chart.
   */
  private JFreeChart createOverlaidChart(String pNdwId, String pStartDate, String pEndDate, TimeSeriesDataContainer tdc) {
    // create plot ...
    // final IntervalXYDataset data1 = createDataset1();
    final DateAxis domainAxis = new DateAxis("Time");
    domainAxis.setTickMarkPosition(DateTickMarkPosition.MIDDLE);
    final ValueAxis rangeAxisTrafficspeed = new NumberAxis("Traffic speed (km/h)");
    TimeSeries trafficspeedSeries = tdc.getTrafficspeedSeries();
    rangeAxisTrafficspeed.setLowerBound(trafficspeedSeries.getMinY());
    rangeAxisTrafficspeed.setUpperBound(1.5 * trafficspeedSeries.getMaxY());
    final IntervalXYDataset dataTrafficspeed = new TimeSeriesCollection(trafficspeedSeries);
    final XYItemRenderer rendererHistogramTraffic = new XYBarRenderer(0.20);
    rendererHistogramTraffic.setBaseToolTipGenerator(
      new StandardXYToolTipGenerator(
        StandardXYToolTipGenerator.DEFAULT_TOOL_TIP_FORMAT,
        new SimpleDateFormat("yyyy-MM-dd"), new DecimalFormat("0.00")
      )
    );
    final XYItemRenderer rendererLineTraffic = new StandardXYItemRenderer();
    rendererLineTraffic.setBaseToolTipGenerator(
      new StandardXYToolTipGenerator(
        StandardXYToolTipGenerator.DEFAULT_TOOL_TIP_FORMAT,
        new SimpleDateFormat("yyyy-MM-dd"), new DecimalFormat("0.00")
      )
    );
    XYPlot plot = new XYPlot(dataTrafficspeed, domainAxis, rangeAxisTrafficspeed, rendererHistogramTraffic);

    final ValueAxis rangeAxisPrecipitation = new NumberAxis("Precipitation (mm/h)");
    TimeSeries precipitationSeries = tdc.getPrecipitationSeries();
    rangeAxisPrecipitation.setLowerBound(-2.5 * precipitationSeries.getMaxY());
    rangeAxisPrecipitation.setUpperBound(1.2 * precipitationSeries.getMaxY());
    plot.setRangeAxis(1, rangeAxisPrecipitation);
    final IntervalXYDataset dataPrecipitation = new TimeSeriesCollection(precipitationSeries);
    final XYItemRenderer rendererHistogramPrecipitation = new XYBarRenderer(0.20);
    rendererHistogramPrecipitation.setBaseToolTipGenerator(
	    new StandardXYToolTipGenerator(
        StandardXYToolTipGenerator.DEFAULT_TOOL_TIP_FORMAT,
        new SimpleDateFormat("yyyy-MM-dd"), new DecimalFormat("0.00")
      )
    );
    plot.setDataset(1, dataPrecipitation);
    plot.mapDatasetToRangeAxis(1, 1);
    plot.setRenderer(1, rendererHistogramPrecipitation);

    // final double x = new Day(9, SerialDate.MARCH, 2002).getMiddleMillisecond();
    // final XYTextAnnotation annotation = new XYTextAnnotation("Hello!", x, 10000.0);
    // annotation.setFont(new Font("SansSerif", Font.PLAIN, 9));
    // plot.addAnnotation(annotation);

    final ValueAxis rangeAxisTemperature = new NumberAxis("Temperature (C)");
    TimeSeries temperatureSeries = tdc.getTemperatureSeries();
    rangeAxisTemperature.setLowerBound(-temperatureSeries.getMaxY());
    rangeAxisTemperature.setUpperBound(1.2 * temperatureSeries.getMaxY());
    plot.setRangeAxis(2, rangeAxisTemperature);
    final IntervalXYDataset dataTemperature = new TimeSeriesCollection(temperatureSeries);
    final XYItemRenderer rendererLineTemperature = new StandardXYItemRenderer();
    rendererLineTemperature.setBaseToolTipGenerator(
      new StandardXYToolTipGenerator(
        StandardXYToolTipGenerator.DEFAULT_TOOL_TIP_FORMAT,
        new SimpleDateFormat("yyyy-MM-dd"), new DecimalFormat("0.00")
      )
    );
    plot.setDataset(2, dataTemperature);
    plot.mapDatasetToRangeAxis(2, 2);
    plot.setRenderer(2, rendererLineTemperature);
    
    final ValueAxis rangeAxisWindspeed = new NumberAxis("Wind speed (m/s)");
    TimeSeries windspeedSeries = tdc.getWindspeedSeries();
    rangeAxisWindspeed.setLowerBound(-2.0 * windspeedSeries.getMaxY());
    rangeAxisWindspeed.setUpperBound(1.2 * windspeedSeries.getMaxY());
    plot.setRangeAxis(3, rangeAxisWindspeed);
    final IntervalXYDataset dataWindspeed = new TimeSeriesCollection(windspeedSeries);
    final XYItemRenderer rendererLineWindspeed = new StandardXYItemRenderer();
    rendererLineWindspeed.setBaseToolTipGenerator(
      new StandardXYToolTipGenerator(
        StandardXYToolTipGenerator.DEFAULT_TOOL_TIP_FORMAT,
        new SimpleDateFormat("yyyy-MM-dd"), new DecimalFormat("0.00")
      )
    );
    plot.setDataset(3, dataWindspeed);
    plot.mapDatasetToRangeAxis(3, 3);
    plot.setRenderer(3, rendererLineWindspeed);

    plot.setDatasetRenderingOrder(DatasetRenderingOrder.FORWARD);

    // return a new chart containing the overlaid plot...
    return new JFreeChart("Weather at measurement site " + pNdwId + " from " + pStartDate + " until " + pEndDate, JFreeChart.DEFAULT_TITLE_FONT, plot, true);

  }
	
}