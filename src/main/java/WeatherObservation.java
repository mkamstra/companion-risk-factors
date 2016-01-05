import java.time.Instant;

/**
 * STN,YYYYMMDD,   HH,   DD,   FH,   FF,   FX,    T,  T10,   TD,   SQ,    Q,   DR,   RH,    P,   VV,    N,    U,   WW,   IX,    M,    R,    S,    O,    Y"
 */
public class WeatherObservation {
	private int mWeatherStationId;
	private Instant mTime;
	private double mTemperature;
}