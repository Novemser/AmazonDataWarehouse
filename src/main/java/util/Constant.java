package util;

/**
 * Project: DataWarehouseServer
 * Package: util
 * Author:  Novemser
 * 2016/12/24
 */
public class Constant {

    public static final String[] seasonMapper = new String[]{
            "Spring",
            "Summer",
            "Autumn",
            "Winter"
    };

    public static final String[] monthMapper = new String[]{
            "Jan", "Feb", "Mar", "Apr", "May", "June", "July", "Aug", "Sept", "Oct", "Nov", "Dec"
    };

    public static final String[] dayMapper = new String[]{
            "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"
    };

    public static final int SEASON_CNT = 4;
    public static final int MONTH_CNT = 12;
    public static final int DAY_CNT = 7;
    public static final int SEASON_AND_DAY = DAY_CNT * SEASON_CNT;
    public static final int DAY_AND_MONTH = DAY_CNT * MONTH_CNT;

    public static final String[] daySeasonMapper = new String[SEASON_AND_DAY];
    public static final String[] dayMonthMapper = new String[DAY_AND_MONTH];

    static {
        for (int i = 0; i < SEASON_CNT; i++) {
            for (int j = 0; j < DAY_CNT; j++) {
                daySeasonMapper[i * DAY_CNT + j] = dayMapper[j] + "_" + seasonMapper[i];
            }
        }

        for (int i = 0; i < MONTH_CNT; i++) {
            for (int j = 0; j < DAY_CNT; j++) {
                dayMonthMapper[i * DAY_CNT + j] = dayMapper[j] + "_" + monthMapper[i];
            }
        }
    }
}
