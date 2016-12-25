package service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSetMetaData;
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
        long start, end;
        JSONObject result = new JSONObject();
        SqlRowSet rowSet = null;
        int count = 0;

        start = System.currentTimeMillis();
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
        end = System.currentTimeMillis();

        if (null != rowSet && rowSet.next()) {
            count = rowSet.getInt(1);
        }
        result.put("year", year);
        result.put("quantity", count);
        result.put("queryTime", (end - start) + "ms");

        return result;
    }

    public JSONObject getMovieCountByYearSeason(JSONObject request, DataBaseType type) {
        JSONObject result = new JSONObject();
        String year = request.getString("year");
        result.put("year", year);
        JSONArray seasons = request.getJSONArray("season");
        long start, end;

        boolean[] seasonQuery = new boolean[Constant.SEASON_CNT];
        for (int i = 0; i < seasons.size(); i++) {
            seasonQuery[i] = seasons.getBoolean(i);
        }

        SqlRowSet rowSet;
        start = System.currentTimeMillis();
        switch (type) {
            case IMPALA:
                for (int i = 1; i <= Constant.SEASON_CNT; i++) {
                    // 不查询
                    if (!seasonQuery[i - 1])
                        continue;
                    if (year.equals("0"))
                        rowSet = impalaTemplate.queryForRowSet(
                                "SELECT count(*) FROM \n" +
                                        "amazonmoviesnapshot AS sh\n" +
                                        "JOIN datedim AS da\n" +
                                        "on sh.date_id = da.date_id\n" +
                                        "WHERE da.date_quarter = " + i
                        );
                    else
                        rowSet = impalaTemplate.queryForRowSet(
                                "SELECT count(*) FROM \n" +
                                        "amazonmoviesnapshot AS sh\n" +
                                        "JOIN datedim AS da\n" +
                                        "on sh.date_id = da.date_id\n" +
                                        "WHERE da.date_year = " + year + " AND da.date_quarter = " + i
                        );

                    end = System.currentTimeMillis();

                    if (rowSet.next()) {
                        String key = Constant.seasonMapper[i - 1];
                        int value = rowSet.getInt(1);
                        result.put(key, value);
                        result.put("queryTime", (end - start) + "ms");
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
                    if (year.equals("0"))
                        rowSet = sqlServerTemplate.queryForRowSet(
                                "SELECT count(*) FROM \n" +
                                        "AmazonMovieDW.db_owner.amazonmoviesnapshot AS sh\n" +
                                        "JOIN AmazonMovieDW.db_owner.datedim AS da\n" +
                                        "on sh.date_id = da.date_id\n" +
                                        "WHERE da.date_quarter = ?", i
                        );
                    else
                        rowSet = sqlServerTemplate.queryForRowSet(
                                "SELECT count(*) FROM \n" +
                                        "AmazonMovieDW.db_owner.amazonmoviesnapshot AS sh\n" +
                                        "JOIN AmazonMovieDW.db_owner.datedim AS da\n" +
                                        "on sh.date_id = da.date_id\n" +
                                        "WHERE da.date_year = ? AND da.date_quarter = ?", year, i
                        );
                    end = System.currentTimeMillis();

                    if (rowSet.next()) {
                        String key = Constant.seasonMapper[i - 1];
                        int value = rowSet.getInt(1);
                        result.put(key, value);
                        result.put("queryTime", (end - start) + "ms");
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
        long start, end;
        JSONArray seasons = request.getJSONArray("month");
        boolean[] monthQuery = new boolean[Constant.MONTH_CNT];
        for (int i = 0; i < seasons.size(); i++) {
            monthQuery[i] = seasons.getBoolean(i);
        }

        start = System.currentTimeMillis();
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
                    end = System.currentTimeMillis();

                    if (rowSet.next()) {
                        String key = Constant.monthMapper[i - 1];
                        int value = rowSet.getInt(1);
                        result.put(key, value);
                        result.put("queryTime", (end - start) + "ms");

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
                    end = System.currentTimeMillis();

                    if (rowSet.next()) {
                        String key = Constant.monthMapper[i - 1];
                        int value = rowSet.getInt(1);
                        result.put(key, value);
                        result.put("queryTime", (end - start) + "ms");
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
        long start, end;
        JSONArray days = request.getJSONArray("day");
        boolean[] dayQuery = new boolean[Constant.DAY_CNT];
        for (int i = 0; i < days.size(); i++) {
            dayQuery[i] = days.getBoolean(i);
        }

        start = System.currentTimeMillis();
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
                    end = System.currentTimeMillis();

                    if (rowSet.next()) {
                        String key = Constant.dayMapper[i - 1];
                        int value = rowSet.getInt(1);
                        result.put(key, value);
                        result.put("queryTime", (end - start) + "ms");

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
                    end = System.currentTimeMillis();

                    if (rowSet.next()) {
                        String key = Constant.dayMapper[i - 1];
                        int value = rowSet.getInt(1);
                        result.put(key, value);
                        result.put("queryTime", (end - start) + "ms");

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
        long start, end;
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

        start = System.currentTimeMillis();
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
                    end = System.currentTimeMillis();

                    if (rowSet.next()) {
                        String key = Constant.daySeasonMapper[i - 1];
                        int value = rowSet.getInt(1);
                        result.put(key, value);
                        result.put("queryTime", (end - start) + "ms");
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
                    end = System.currentTimeMillis();

                    if (rowSet.next()) {
                        String key = Constant.daySeasonMapper[i - 1];
                        int value = rowSet.getInt(1);
                        result.put(key, value);
                        result.put("queryTime", (end - start) + "ms");
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
        long start, end;
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

        start = System.currentTimeMillis();
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
                    end = System.currentTimeMillis();

                    if (rowSet.next()) {
                        String key = Constant.dayMonthMapper[i - 1];
                        int value = rowSet.getInt(1);
                        result.put(key, value);
                        result.put("queryTime", (end - start) + "ms");
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
                    end = System.currentTimeMillis();

                    if (rowSet.next()) {
                        String key = Constant.dayMonthMapper[i - 1];
                        int value = rowSet.getInt(1);
                        result.put(key, value);
                        result.put("queryTime", (end - start) + "ms");
                    }
                }
                break;
            case HIVE:
                break;
        }

        return result;
    }

    public JSONObject getMovieByNameLike(JSONObject request, DataBaseType type) {
        String name = request.getString("name");
        JSONObject result = new JSONObject();
        JSONArray array = new JSONArray();
        SqlRowSet rowSet = null;
        long start, end = 0;
        start = System.currentTimeMillis();
        switch (type) {
            case IMPALA:
                rowSet = impalaTemplate.queryForRowSet(
                        "SELECT * FROM amazonmoviesnapshot WHERE lower(title) LIKE lower('%" + name + "%')"
                );
                end = System.currentTimeMillis();
                break;
            case MYSQL:
                break;
            case MSSQLSERVER:
                rowSet = sqlServerTemplate.queryForRowSet(
                        "SELECT * FROM AmazonMovieDW.db_owner.amazonmoviesnapshot WHERE title LIKE '%" + name + "%'"
                );
                end = System.currentTimeMillis();
                break;
            case HIVE:
                break;
        }

        SqlRowSetMetaData metaData = rowSet.getMetaData();

        while (rowSet.next()) {
            JSONObject movie = new JSONObject();

            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String col = metaData.getColumnName(i);
                String val = rowSet.getString(col);
                movie.put(col, val);
            }

            array.add(movie);
        }
        result.put("queryTime", (end - start) + "ms");
        result.put("dbType", type);
        result.put("movies", array);
        result.put("count", array.size());

        return result;
    }

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
    public JSONObject listAllMovieCountByYear(DataBaseType type) {
        JSONObject result = new JSONObject();
        JSONArray array = new JSONArray();
        SqlRowSet rowSet = null;
        long start, end = 0;
        start = System.currentTimeMillis();
        switch (type) {
            case IMPALA:
                rowSet = impalaTemplate.queryForRowSet(
                        "SELECT date_year,COUNT(*) FROM datedim AS da JOIN amazonmoviesnapshot AS sh\n" +
                                "ON da.date_id = sh.date_id\n" +
                                "group by date_year\n" +
                                "order by date_year"
                );
                break;
            case MYSQL:
                break;
            case MSSQLSERVER:
                rowSet = sqlServerTemplate.queryForRowSet(
                        "SELECT date_year,count(*) FROM AmazonMovieDW.db_owner.datedim AS da " +
                                "JOIN AmazonMovieDW.db_owner.amazonmoviesnapshot AS sh " +
                                "ON da.date_id = sh.date_id " +
                                "GROUP BY date_year " +
                                "ORDER BY date_year"
                );
                break;
            case HIVE:
                break;
        }
        while (rowSet.next()) {
            String year = rowSet.getString(1);
            String count = rowSet.getString(2);
            result.put(year, count);
        }

        return result;
    }
}
