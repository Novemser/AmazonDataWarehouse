package service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Service;
import util.Constant;
import util.DataBaseType;

/**
 * Project: DataWarehouseServer
 * Package: service
 * Author:  Novemser
 * 2016/12/24
 */

@Service
public class BaseService {
    @Autowired
    @Qualifier("sqlServerJdbcTemplate")
    private JdbcTemplate sqlServerTemplate;

    @Autowired
    @Qualifier("mySqlJdbcTemplate")
    private JdbcTemplate mySqlTemplate;

    @Autowired
    @Qualifier("hiveJdbcTemplate")
    private JdbcTemplate hiveTemplate;

    @Autowired
    @Qualifier("impalaJdbcTemplate")
    private JdbcTemplate impalaTemplate;

    public JSONObject getMovieCountByYear(JSONObject request, DataBaseType type) {
        String year = request.getString("year");
        // 修改了表以后不用设置了
        // 这里时间戳一定要设好16小时的误差
//        String hdfsYear = year;
        JSONObject result = new JSONObject();
        SqlRowSet rowSet = null;
        int count = 0;

        switch (type) {
            case HIVE:
                break;
            case IMPALA:
                rowSet = impalaTemplate.queryForRowSet(
                        "SELECT COUNT(*) FROM datedim AS da JOIN amazonmoviesnapshot AS sh " +
                                "ON da.date_id = sh.date_id " +
                                "WHERE date_year = " + year
                );
                break;
            case MSSQLSERVER:
                rowSet = sqlServerTemplate.queryForRowSet(
                        "SELECT count(*) FROM AmazonMovieDW.db_owner.datedim AS da " +
                                "JOIN AmazonMovieDW.db_owner.amazonmoviesnapshot AS sh " +
                                "ON da.date_id = sh.date_id " +
                                "WHERE da.date_year =" + year
                );
                break;
            case MYSQL:
                break;
        }

        if (null != rowSet && rowSet.next()) {
            count = rowSet.getInt(1);
        }
        result.put("year", year);
        result.put("quantity", count);

        return result;
    }

    public JSONObject getMovieCountByYearSeason(JSONObject request, DataBaseType type) {
        JSONObject result = new JSONObject();
        String year = request.getString("year");
        result.put("year", year);
        JSONArray seasons = request.getJSONArray("season");
        boolean[] seasonQuery = new boolean[Constant.SEASON_CNT];
        for (int i = 0; i < seasons.size(); i++) {
            seasonQuery[i] = seasons.getBoolean(i);
        }

        SqlRowSet rowSet;

        switch (type) {
            case IMPALA:
                for (int i = 1; i <= Constant.SEASON_CNT; i++) {
                    // 不查询
                    if (!seasonQuery[i - 1])
                        continue;

                    // 查询
                    rowSet = impalaTemplate.queryForRowSet(
                            "SELECT count(*) FROM \n" +
                                    "amazonmoviesnapshot AS sh\n" +
                                    "JOIN datedim AS da\n" +
                                    "on sh.date_id = da.date_id\n" +
                                    "WHERE da.date_year = " + year + " AND da.date_quarter = " + i
                    );

                    if (rowSet.next()) {
                        String key = Constant.seasonMapper[i - 1];
                        int value = rowSet.getInt(1);
                        result.put(key, value);
                    }
                }
                break;
            case MYSQL:
                break;
            case MSSQLSERVER:
                for (int i = 1; i <= Constant.SEASON_CNT; i++) {
                    // 不查询
                    if (!seasonQuery[i - 1])
                        continue;

                    // 查询
                    rowSet = sqlServerTemplate.queryForRowSet(
                            "SELECT count(*) FROM \n" +
                                    "AmazonMovieDW.db_owner.amazonmoviesnapshot AS sh\n" +
                                    "JOIN AmazonMovieDW.db_owner.datedim AS da\n" +
                                    "on sh.date_id = da.date_id\n" +
                                    "WHERE da.date_year = ? AND da.date_quarter = ?", year, i
                    );

                    if (rowSet.next()) {
                        String key = Constant.seasonMapper[i - 1];
                        int value = rowSet.getInt(1);
                        result.put(key, value);
                    }
                }
                break;
            case HIVE:
                break;
        }

        return result;
    }

    public JSONObject getMovieCountByYearMonth(JSONObject request, DataBaseType type) {
        JSONObject result = new JSONObject();
        String year = request.getString("year");
        result.put("year", year);
        SqlRowSet rowSet;
        JSONArray seasons = request.getJSONArray("month");
        boolean[] monthQuery = new boolean[Constant.MONTH_CNT];
        for (int i = 0; i < seasons.size(); i++) {
            monthQuery[i] = seasons.getBoolean(i);
        }

        switch (type) {
            case IMPALA:
                for (int i = 1; i <= Constant.MONTH_CNT; i++) {
                    // 不查询
                    if (!monthQuery[i - 1])
                        continue;

                    // 查询
                    rowSet = impalaTemplate.queryForRowSet(
                            "SELECT count(*) FROM \n" +
                                    "amazonmoviesnapshot AS sh\n" +
                                    "JOIN datedim AS da\n" +
                                    "on sh.date_id = da.date_id\n" +
                                    "WHERE da.date_year = " + year + " AND da.date_month = " + i
                    );

                    if (rowSet.next()) {
                        String key = Constant.monthMapper[i - 1];
                        int value = rowSet.getInt(1);
                        result.put(key, value);
                    }
                }
                break;
            case MYSQL:
                break;
            case MSSQLSERVER:
                for (int i = 1; i <= Constant.MONTH_CNT; i++) {
                    // 不查询
                    if (!monthQuery[i - 1])
                        continue;

                    // 查询
                    rowSet = sqlServerTemplate.queryForRowSet(
                            "SELECT count(*) FROM \n" +
                                    "AmazonMovieDW.db_owner.amazonmoviesnapshot AS sh\n" +
                                    "JOIN AmazonMovieDW.db_owner.datedim AS da\n" +
                                    "on sh.date_id = da.date_id\n" +
                                    "WHERE da.date_year = ? AND da.date_month = ?", year, i
                    );

                    if (rowSet.next()) {
                        String key = Constant.monthMapper[i - 1];
                        int value = rowSet.getInt(1);
                        result.put(key, value);
                    }
                }
                break;
            case HIVE:
                break;
        }

        return result;
    }

    public JSONObject getMovieCountByYearDay(JSONObject request, DataBaseType type) {
        JSONObject result = new JSONObject();
        String year = request.getString("year");
        result.put("year", year);
        SqlRowSet rowSet;
        JSONArray days = request.getJSONArray("day");
        boolean[] dayQuery = new boolean[Constant.DAY_CNT];
        for (int i = 0; i < days.size(); i++) {
            dayQuery[i] = days.getBoolean(i);
        }

        switch (type) {
            case IMPALA:
                for (int i = 1; i <= Constant.DAY_CNT; i++) {
                    // 不查询
                    if (!dayQuery[i - 1])
                        continue;

                    // 查询
                    rowSet = impalaTemplate.queryForRowSet(
                            "SELECT count(*) FROM \n" +
                                    "amazonmoviesnapshot AS sh\n" +
                                    "JOIN datedim AS da\n" +
                                    "on sh.date_id = da.date_id\n" +
                                    "WHERE da.date_year = " + year + " AND da.date_day_of_week = " + i
                    );

                    if (rowSet.next()) {
                        String key = Constant.dayMapper[i - 1];
                        int value = rowSet.getInt(1);
                        result.put(key, value);
                    }
                }
                break;
            case MYSQL:
                break;
            case MSSQLSERVER:
                for (int i = 1; i <= Constant.DAY_CNT; i++) {
                    // 不查询
                    if (!dayQuery[i - 1])
                        continue;

                    // 查询
                    rowSet = sqlServerTemplate.queryForRowSet(
                            "SELECT count(*) FROM \n" +
                                    "AmazonMovieDW.db_owner.amazonmoviesnapshot AS sh\n" +
                                    "JOIN AmazonMovieDW.db_owner.datedim AS da\n" +
                                    "on sh.date_id = da.date_id\n" +
                                    "WHERE da.date_year = ? AND da.date_day_of_week = ?", year, i
                    );

                    if (rowSet.next()) {
                        String key = Constant.dayMapper[i - 1];
                        int value = rowSet.getInt(1);
                        result.put(key, value);
                    }
                }
                break;
            case HIVE:
                break;
        }

        return result;
    }

    public JSONObject getMovieCountByYearSeasonDay(JSONObject request, DataBaseType type) {
        JSONObject result = new JSONObject();
        String year = request.getString("year");
        result.put("year", year);
        SqlRowSet rowSet;
        JSONArray seasons = request.getJSONArray("season");
        JSONArray days = request.getJSONArray("day");
        boolean[] seasonDayQuery = new boolean[Constant.SEASON_AND_DAY];
        for (int i = 0; i < Constant.SEASON_CNT; i++) {
            if (seasons.getBoolean(i)) {
                for (int j = 0; j < Constant.DAY_CNT; j++) {
                    if (days.getBoolean(j)) {
                        seasonDayQuery[i * Constant.DAY_CNT + j] = true;
                    }
                }
            }
        }

        switch (type) {
            case IMPALA:
                for (int i = 1; i <= Constant.SEASON_AND_DAY; i++) {
                    // 不查询
                    if (!seasonDayQuery[i - 1])
                        continue;

                    // 查询
                    rowSet = impalaTemplate.queryForRowSet(
                            "SELECT count(*) FROM \n" +
                                    "amazonmoviesnapshot AS sh\n" +
                                    "JOIN datedim AS da\n" +
                                    "on sh.date_id = da.date_id\n" +
                                    "WHERE da.date_year = " + year +
                                    " AND da.date_day_of_week = " +
                                    String.valueOf((i - 1) % Constant.DAY_CNT + 1) +
                                    " AND da.date_quarter = " +
                                    String.valueOf((i - 1) / Constant.DAY_CNT + 1)
                    );

                    if (rowSet.next()) {
                        String key = Constant.daySeasonMapper[i - 1];
                        int value = rowSet.getInt(1);
                        result.put(key, value);
                    }
                }
                break;
            case MYSQL:
                break;
            case MSSQLSERVER:
                for (int i = 1; i <= Constant.SEASON_AND_DAY; i++) {
                    // 不查询
                    if (!seasonDayQuery[i - 1])
                        continue;

                    // 查询
                    rowSet = sqlServerTemplate.queryForRowSet(
                            "SELECT count(*) FROM \n" +
                                    "AmazonMovieDW.db_owner.amazonmoviesnapshot AS sh\n" +
                                    "JOIN AmazonMovieDW.db_owner.datedim AS da\n" +
                                    "on sh.date_id = da.date_id\n" +
                                    "WHERE da.date_year = ? AND da.date_day_of_week = ? AND da.date_quarter = ?",
                            year, (i - 1) % Constant.DAY_CNT + 1, (i - 1) / Constant.DAY_CNT + 1
                    );

                    if (rowSet.next()) {
                        String key = Constant.daySeasonMapper[i - 1];
                        int value = rowSet.getInt(1);
                        result.put(key, value);
                    }
                }
                break;
            case HIVE:
                break;
        }

        return result;
    }

    public JSONObject getMovieCountByYearMonthDay(JSONObject request, DataBaseType type) {
        JSONObject result = new JSONObject();
        String year = request.getString("year");
        result.put("year", year);
        SqlRowSet rowSet;
        JSONArray months = request.getJSONArray("month");
        JSONArray days = request.getJSONArray("day");
        boolean[] monthDayQuery = new boolean[Constant.DAY_AND_MONTH];
        for (int i = 0; i < Constant.MONTH_CNT; i++) {
            if (months.getBoolean(i)) {
                for (int j = 0; j < Constant.DAY_CNT; j++) {
                    if (days.getBoolean(j)) {
                        monthDayQuery[i * Constant.DAY_CNT + j] = true;
                    }
                }
            }
        }

        switch (type) {
            case IMPALA:
                for (int i = 1; i <= Constant.DAY_AND_MONTH; i++) {
                    // 不查询
                    if (!monthDayQuery[i - 1])
                        continue;

                    // 查询
                    rowSet = impalaTemplate.queryForRowSet(
                            "SELECT count(*) FROM \n" +
                                    "amazonmoviesnapshot AS sh\n" +
                                    "JOIN datedim AS da\n" +
                                    "on sh.date_id = da.date_id\n" +
                                    "WHERE da.date_year = " + year +
                                    " AND da.date_day_of_week = " +
                                    String.valueOf((i - 1) % Constant.DAY_CNT + 1) +
                                    " AND da.date_month = " +
                                    String.valueOf((i - 1) / Constant.DAY_CNT + 1)
                    );

                    if (rowSet.next()) {
                        String key = Constant.dayMonthMapper[i - 1];
                        int value = rowSet.getInt(1);
                        result.put(key, value);
                    }
                }
                break;
            case MYSQL:
                break;
            case MSSQLSERVER:
                for (int i = 1; i <= Constant.DAY_AND_MONTH; i++) {
                    // 不查询
                    if (!monthDayQuery[i - 1])
                        continue;

                    // 查询
                    rowSet = sqlServerTemplate.queryForRowSet(
                            "SELECT count(*) FROM \n" +
                                    "AmazonMovieDW.db_owner.amazonmoviesnapshot AS sh\n" +
                                    "JOIN AmazonMovieDW.db_owner.datedim AS da\n" +
                                    "on sh.date_id = da.date_id\n" +
                                    "WHERE da.date_year = ? AND da.date_day_of_week = ? AND da.date_month = ?",
                            year, (i - 1) % Constant.DAY_CNT + 1, (i - 1) / Constant.DAY_CNT + 1
                    );

                    if (rowSet.next()) {
                        String key = Constant.dayMonthMapper[i - 1];
                        int value = rowSet.getInt(1);
                        result.put(key, value);
                    }
                }
                break;
            case HIVE:
                break;
        }

        return result;
    }
//
//    JSONObject getDirectorById(JSONObject request);
//
//    JSONObject getActorById(JSONObject request);
//
//    JSONObject getMovieByCategoryId(JSONObject request);
    //    switch (type) {
//        case IMPALA:
//            break;
//        case MYSQL:
//            break;
//        case MSSQLSERVER:
//            break;
//        case HIVE:
//            break;
//    }
//
}
