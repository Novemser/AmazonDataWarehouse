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
        result.put("year", year);

        start = System.currentTimeMillis();
        switch (type) {
            case HIVE:
                break;
            case IMPALA:
                if (year.equals("0"))
                    year = "date_year";
                rowSet = impalaTemplate.queryForRowSet(
                        "SELECT COUNT(*) FROM datedim AS da JOIN amazonmoviesnapshot AS sh " +
                                "ON da.date_id = sh.date_id " +
                                "WHERE date_year = " + year
                );
                break;
            case MSSQLSERVER:
                if (year.equals("0"))
                    year = "da.date_year";

                rowSet = sqlServerTemplate.queryForRowSet(
                        "SELECT count(*) FROM AmazonMovieDW.MVMK.datedim AS da " +
                                "JOIN AmazonMovieDW.MVMK.amazonmoviesnapshot AS sh " +
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
                                "SELECT count(*) FROM  " +
                                        "amazonmoviesnapshot AS sh " +
                                        "JOIN datedim AS da " +
                                        "on sh.date_id = da.date_id " +
                                        "WHERE da.date_quarter = " + i
                        );
                    else
                        rowSet = impalaTemplate.queryForRowSet(
                                "SELECT count(*) FROM  " +
                                        "amazonmoviesnapshot AS sh " +
                                        "JOIN datedim AS da " +
                                        "on sh.date_id = da.date_id " +
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
                                "SELECT count(*) FROM  " +
                                        "AmazonMovieDW.MVMK.amazonmoviesnapshot AS sh " +
                                        "JOIN AmazonMovieDW.MVMK.datedim AS da " +
                                        "on sh.date_id = da.date_id " +
                                        "WHERE da.date_quarter = ?", i
                        );
                    else
                        rowSet = sqlServerTemplate.queryForRowSet(
                                "SELECT count(*) FROM  " +
                                        "AmazonMovieDW.MVMK.amazonmoviesnapshot AS sh " +
                                        "JOIN AmazonMovieDW.MVMK.datedim AS da " +
                                        "on sh.date_id = da.date_id " +
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
                    if (year.equals("0"))
                        rowSet = impalaTemplate.queryForRowSet(
                                "SELECT count(*) FROM  " +
                                        "amazonmoviesnapshot AS sh " +
                                        "JOIN datedim AS da " +
                                        "on sh.date_id = da.date_id " +
                                        "WHERE da.date_month = " + i
                        );
                    else
                        rowSet = impalaTemplate.queryForRowSet(
                                "SELECT count(*) FROM  " +
                                        "amazonmoviesnapshot AS sh " +
                                        "JOIN datedim AS da " +
                                        "on sh.date_id = da.date_id " +
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
                    if (year.equals("0"))
                        rowSet = sqlServerTemplate.queryForRowSet(
                                "SELECT count(*) FROM  " +
                                        "AmazonMovieDW.MVMK.amazonmoviesnapshot AS sh " +
                                        "JOIN AmazonMovieDW.MVMK.datedim AS da " +
                                        "on sh.date_id = da.date_id " +
                                        "WHERE da.date_month = ?", i
                        );
                    else
                        rowSet = sqlServerTemplate.queryForRowSet(
                                "SELECT count(*) FROM  " +
                                        "AmazonMovieDW.MVMK.amazonmoviesnapshot AS sh " +
                                        "JOIN AmazonMovieDW.MVMK.datedim AS da " +
                                        "on sh.date_id = da.date_id " +
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
                    if (year.equals("0"))
                        rowSet = impalaTemplate.queryForRowSet(
                                "SELECT count(*) FROM  " +
                                        "amazonmoviesnapshot AS sh " +
                                        "JOIN datedim AS da " +
                                        "on sh.date_id = da.date_id " +
                                        "WHERE da.date_day_of_week = " + i
                        );
                    else
                        rowSet = impalaTemplate.queryForRowSet(
                                "SELECT count(*) FROM  " +
                                        "amazonmoviesnapshot AS sh " +
                                        "JOIN datedim AS da " +
                                        "on sh.date_id = da.date_id " +
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
                    if (year.equals("0"))
                        rowSet = sqlServerTemplate.queryForRowSet(
                                "SELECT count(*) FROM  " +
                                        "AmazonMovieDW.MVMK.amazonmoviesnapshot AS sh " +
                                        "JOIN AmazonMovieDW.MVMK.datedim AS da " +
                                        "on sh.date_id = da.date_id " +
                                        "WHERE da.date_day_of_week = ?", i
                        );
                    else
                        rowSet = sqlServerTemplate.queryForRowSet(
                                "SELECT count(*) FROM  " +
                                        "AmazonMovieDW.MVMK.amazonmoviesnapshot AS sh " +
                                        "JOIN AmazonMovieDW.MVMK.datedim AS da " +
                                        "on sh.date_id = da.date_id " +
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
                    if (year.equals("0"))
                        year = "da.date_year";
                    rowSet = impalaTemplate.queryForRowSet(
                            "SELECT count(*) FROM  " +
                                    "amazonmoviesnapshot AS sh " +
                                    "JOIN datedim AS da " +
                                    "on sh.date_id = da.date_id " +
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
                    if (year.equals("0"))
                        rowSet = sqlServerTemplate.queryForRowSet(
                                "SELECT count(*) FROM  " +
                                        "AmazonMovieDW.MVMK.amazonmoviesnapshot AS sh " +
                                        "JOIN AmazonMovieDW.MVMK.datedim AS da " +
                                        "on sh.date_id = da.date_id " +
                                        "WHERE da.date_day_of_week = ? AND da.date_quarter = ?",
                                (i - 1) % Constant.DAY_CNT + 1, (i - 1) / Constant.DAY_CNT + 1
                        );
                    else
                        rowSet = sqlServerTemplate.queryForRowSet(
                                "SELECT count(*) FROM  " +
                                        "AmazonMovieDW.MVMK.amazonmoviesnapshot AS sh " +
                                        "JOIN AmazonMovieDW.MVMK.datedim AS da " +
                                        "on sh.date_id = da.date_id " +
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
                    if (year.equals("0"))
                        year = "da.date_year";
                    rowSet = impalaTemplate.queryForRowSet(
                            "SELECT count(*) FROM  " +
                                    "amazonmoviesnapshot AS sh " +
                                    "JOIN datedim AS da " +
                                    "on sh.date_id = da.date_id " +
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
                    if (year.equals("0"))
                        rowSet = sqlServerTemplate.queryForRowSet(
                                "SELECT count(*) FROM  " +
                                        "AmazonMovieDW.MVMK.amazonmoviesnapshot AS sh " +
                                        "JOIN AmazonMovieDW.MVMK.datedim AS da " +
                                        "on sh.date_id = da.date_id " +
                                        "WHERE da.date_day_of_week = ? AND da.date_month = ?",
                                (i - 1) % Constant.DAY_CNT + 1, (i - 1) / Constant.DAY_CNT + 1
                        );
                    else
                        rowSet = sqlServerTemplate.queryForRowSet(
                                "SELECT count(*) FROM  " +
                                        "AmazonMovieDW.MVMK.amazonmoviesnapshot AS sh " +
                                        "JOIN AmazonMovieDW.MVMK.datedim AS da " +
                                        "on sh.date_id = da.date_id " +
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
                break;
            case MYSQL:
                break;
            case MSSQLSERVER:
                rowSet = sqlServerTemplate.queryForRowSet(
                        "SELECT * FROM AmazonMovieDW.MVMK.amazonmoviesnapshot WHERE title LIKE '%" + name + "%'"
                );
                break;
            case HIVE:
                break;
        }
        end = System.currentTimeMillis();

        SqlRowSetMetaData metaData = rowSet.getMetaData();

        getData(array, rowSet, metaData);
        result.put("queryTime", (end - start) + "ms");
        result.put("dbType", type);
        result.put("movies", array);
        result.put("count", array.size());

        return result;
    }

    public JSONObject getActorMoviesById(JSONObject request, DataBaseType type) {
        JSONObject result = new JSONObject();
        String id = request.getString("actor_id");
        boolean staring = request.getBoolean("staring");
        boolean supporting = request.getBoolean("supporting");
        SqlRowSet rowSet;
        long start, time = 0;

        switch (type) {
            case IMPALA:
                if (staring) {
                    JSONArray staringArray = new JSONArray();

                    start = System.currentTimeMillis();
                    rowSet = impalaTemplate.queryForRowSet(
                            "SELECT * FROM  " +
                                    "amazonmoviesnapshot AS sh " +
                                    "JOIN movietoactorbridge AS ab " +
                                    "ON sh.snapshot_id = ab.AmazonMovieSnapshot_snapshot_id " +
                                    "JOIN actordim AS ad " +
                                    "ON ab.ActorDim_actor_id = ad.actor_id " +
                                    "WHERE ad.actor_id = " + id
                    );
                    time = System.currentTimeMillis() - start;

                    SqlRowSetMetaData metaData = rowSet.getMetaData();

                    getData(staringArray, rowSet, metaData);

                    result.put("staring", staringArray);
                }
                if (supporting) {
                    JSONArray supportingArray = new JSONArray();
                    start = System.currentTimeMillis();
                    rowSet = impalaTemplate.queryForRowSet(
                            "SELECT * FROM  " +
                                    "amazonmoviesnapshot AS sh " +
                                    "JOIN movietosupportingactorbridge AS ab " +
                                    "ON sh.snapshot_id = ab.AmazonMovieSnapshot_snapshot_id " +
                                    "JOIN actordim AS ad " +
                                    "ON ab.ActorDim_actor_id = ad.actor_id " +
                                    "WHERE ad.actor_id = " + id
                    );
                    time += System.currentTimeMillis() - start;

                    SqlRowSetMetaData metaData = rowSet.getMetaData();

                    getData(supportingArray, rowSet, metaData);

                    result.put("supporting", supportingArray);
                }
                break;
            case MYSQL:
                break;
            case MSSQLSERVER:
                if (staring) {
                    JSONArray staringArray = new JSONArray();

                    start = System.currentTimeMillis();
                    rowSet = sqlServerTemplate.queryForRowSet(
                            "SELECT * FROM  " +
                                    "AmazonMovieDW.MVMK.amazonmoviesnapshot AS sh " +
                                    "JOIN AmazonMovieDW.MVMK.movietoactorbridge AS ab " +
                                    "ON sh.snapshot_id = ab.AmazonMovieSnapshot_snapshot_id " +
                                    "JOIN AmazonMovieDW.MVMK.actordim AS ad " +
                                    "ON ab.ActorDim_actor_id = ad.actor_id " +
                                    "WHERE ad.actor_id = ?", id
                    );
                    time = System.currentTimeMillis() - start;

                    SqlRowSetMetaData metaData = rowSet.getMetaData();

                    getData(staringArray, rowSet, metaData);

                    result.put("staring", staringArray);
                }
                if (supporting) {
                    JSONArray supportingArray = new JSONArray();
                    start = System.currentTimeMillis();
                    rowSet = sqlServerTemplate.queryForRowSet(
                            "SELECT * FROM  " +
                                    "AmazonMovieDW.MVMK.amazonmoviesnapshot AS sh " +
                                    "JOIN AmazonMovieDW.MVMK.movietosupportingactorbridge AS ab " +
                                    "ON sh.snapshot_id = ab.AmazonMovieSnapshot_snapshot_id " +
                                    "JOIN AmazonMovieDW.MVMK.actordim AS ad " +
                                    "ON ab.ActorDim_actor_id = ad.actor_id " +
                                    "WHERE ad.actor_id = ?", id
                    );
                    time += System.currentTimeMillis() - start;

                    SqlRowSetMetaData metaData = rowSet.getMetaData();

                    getData(supportingArray, rowSet, metaData);

                    result.put("supporting", supportingArray);
                }
                break;
            case HIVE:
                break;
        }
        result.put("queryTime", time);
        return result;
    }

    public JSONObject getDirectorsMovieById(JSONObject request, DataBaseType type) {
        JSONObject result = new JSONObject();
        JSONArray array = new JSONArray();
        String id = request.getString("director_id");
        SqlRowSet rowSet = null;
        long start, end = 0;
        start = System.currentTimeMillis();
        switch (type) {
            case IMPALA:
                rowSet = impalaTemplate.queryForRowSet(
                        "SELECT * FROM  " +
                                "amazonmoviesnapshot AS sh " +
                                "JOIN movietodirectorbridge AS ab " +
                                "ON sh.snapshot_id = ab.AmazonMovieSnapshot_snapshot_id " +
                                "JOIN directordim AS ad " +
                                "ON ab.DirectorDim_director_id = ad.director_id " +
                                "WHERE ad.director_id = " + id
                );
                break;
            case MYSQL:
                break;
            case MSSQLSERVER:
                rowSet = sqlServerTemplate.queryForRowSet(
                        "SELECT * FROM  " +
                                "AmazonMovieDW.MVMK.amazonmoviesnapshot AS sh " +
                                "JOIN AmazonMovieDW.MVMK.movietodirectorbridge AS ab " +
                                "ON sh.snapshot_id = ab.AmazonMovieSnapshot_snapshot_id " +
                                "JOIN AmazonMovieDW.MVMK.directordim AS ad " +
                                "ON ab.DirectorDim_director_id = ad.director_id " +
                                "WHERE ad.director_id = ?", id
                );

                break;
            case HIVE:
                break;
        }
        end = System.currentTimeMillis();

        SqlRowSetMetaData metaData = rowSet.getMetaData();

        getData(array, rowSet, metaData);

        result.put("queryTime", (end - start) + "ms");
        result.put("movie", array);
        return result;
    }

    public JSONObject getMovieByCategoryId(JSONObject request, DataBaseType type) {
        JSONObject result = new JSONObject();
        JSONArray array = new JSONArray();
        String id = request.getString("category_id");
        SqlRowSet rowSet = null;
        long start, end = 0;
        start = System.currentTimeMillis();
        switch (type) {
            case IMPALA:
                rowSet = impalaTemplate.queryForRowSet(
                        "SELECT * FROM  " +
                                "amazonmoviesnapshot AS sh " +
                                "JOIN moviecategorybridge AS ab " +
                                "ON sh.snapshot_id = ab.AmazonMovieSnapshot_snapshot_id " +
                                "JOIN categorydim AS ad " +
                                "ON ab.CategoryDim_category_id = ad.category_id " +
                                "WHERE ad.category_id = " + id + " LIMIT 100"
                );
                break;
            case MYSQL:
                break;
            case MSSQLSERVER:
                rowSet = sqlServerTemplate.queryForRowSet(
                        "SELECT TOP 100 * FROM  " +
                                "AmazonMovieDW.MVMK.amazonmoviesnapshot AS sh " +
                                "JOIN AmazonMovieDW.MVMK.moviecategorybridge AS ab " +
                                "ON sh.snapshot_id = ab.AmazonMovieSnapshot_snapshot_id " +
                                "JOIN AmazonMovieDW.MVMK.categorydim AS ad " +
                                "ON ab.CategoryDim_category_id = ad.category_id " +
                                "WHERE ad.category_id = ?", id
                );

                break;
            case HIVE:
                break;
        }
        end = System.currentTimeMillis();

        SqlRowSetMetaData metaData = rowSet.getMetaData();

        getData(array, rowSet, metaData);

        result.put("queryTime", (end - start) + "ms");
        result.put("movie", array);
        return result;
    }

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
        SqlRowSet rowSet = null;
        long start, end;
        start = System.currentTimeMillis();
        switch (type) {
            case IMPALA:
                rowSet = impalaTemplate.queryForRowSet(
                        "SELECT date_year,COUNT(*) FROM datedim AS da JOIN amazonmoviesnapshot AS sh " +
                                "ON da.date_id = sh.date_id " +
                                "group by date_year " +
                                "order by date_year"
                );
                break;
            case MYSQL:
                break;
            case MSSQLSERVER:
                rowSet = sqlServerTemplate.queryForRowSet(
                        "SELECT date_year,count(*) FROM AmazonMovieDW.MVMK.datedim AS da " +
                                "JOIN AmazonMovieDW.MVMK.amazonmoviesnapshot AS sh " +
                                "ON da.date_id = sh.date_id " +
                                "GROUP BY date_year " +
                                "ORDER BY date_year"
                );
                break;
            case HIVE:
                break;
        }
        end = System.currentTimeMillis();
        while (rowSet.next()) {
            String year = rowSet.getString(1);
            String count = rowSet.getString(2);
            result.put(year, count);
        }
        result.put("queryTime", (end - start) + "ms");

        return result;
    }

    public JSONObject listMovieByRanking(JSONObject request, DataBaseType type) {
        JSONObject result = new JSONObject();
        String min = request.getString("min");
        String max = request.getString("max");
        JSONArray array = new JSONArray();
        SqlRowSet rowSet = null;
        long start = System.currentTimeMillis();
        switch (type) {
            case IMPALA:
                rowSet = impalaTemplate.queryForRowSet(
                        "SELECT count(*) FROM amazonmoviesnapshot WHERE ranking BETWEEN " + min + " AND " + max
                );

                if (rowSet.next())
                    result.put("count", rowSet.getInt(1));

                rowSet = impalaTemplate.queryForRowSet(
                        "SELECT * FROM amazonmoviesnapshot WHERE ranking BETWEEN " + min + " AND " + max + " LIMIT 100"
                );
                break;
            case MYSQL:
                break;
            case MSSQLSERVER:
                rowSet = sqlServerTemplate.queryForRowSet(
                        "SELECT count(*) FROM AmazonMovieDW.MVMK.amazonmoviesnapshot WHERE ranking BETWEEN ? AND ?",
                        min, max
                );

                if (rowSet.next())
                    result.put("count", rowSet.getInt(1));

                rowSet = sqlServerTemplate.queryForRowSet(
                        "SELECT TOP 100 * FROM AmazonMovieDW.MVMK.amazonmoviesnapshot WHERE ranking BETWEEN ? AND ?",
                        min, max
                );
                break;
            case HIVE:
                break;
        }
        long end = System.currentTimeMillis();

        SqlRowSetMetaData metaData = rowSet.getMetaData();

        getData(array, rowSet, metaData);

        result.put("queryTime", (end - start) + "ms");
        result.put("movies", array);

        return result;
    }

    public JSONObject listMovieByReview(JSONObject request, DataBaseType type) {
        JSONObject result = new JSONObject();
        String min = request.getString("min");
        String max = request.getString("max");
        JSONArray array = new JSONArray();
        SqlRowSet rowSet = null;
        long start = System.currentTimeMillis();
        switch (type) {
            case IMPALA:
                rowSet = impalaTemplate.queryForRowSet(
                        "SELECT count(*) FROM amazonmoviesnapshot WHERE avg_cus_review BETWEEN " + min + " AND " + max
                );

                if (rowSet.next())
                    result.put("count", rowSet.getInt(1));

                rowSet = impalaTemplate.queryForRowSet(
                        "SELECT * FROM amazonmoviesnapshot WHERE avg_cus_review BETWEEN " + min + " AND " + max + " LIMIT 100"
                );
                break;
            case MYSQL:
                break;
            case MSSQLSERVER:
                rowSet = sqlServerTemplate.queryForRowSet(
                        "SELECT count(*) FROM AmazonMovieDW.MVMK.amazonmoviesnapshot WHERE avg_cus_review BETWEEN ? AND ?",
                        min, max
                );

                if (rowSet.next())
                    result.put("count", rowSet.getInt(1));

                rowSet = sqlServerTemplate.queryForRowSet(
                        "SELECT TOP 100 * FROM AmazonMovieDW.MVMK.amazonmoviesnapshot WHERE avg_cus_review BETWEEN ? AND ?",
                        min, max
                );
                break;
            case HIVE:
                break;
        }
        long end = System.currentTimeMillis();

        SqlRowSetMetaData metaData = rowSet.getMetaData();

        getData(array, rowSet, metaData);

        result.put("queryTime", (end - start) + "ms");
        result.put("movies", array);

        return result;
    }

    public JSONObject listMovieByActorIdAndYear(JSONObject request, DataBaseType type) {
        JSONObject result = new JSONObject();

        String actorId = request.getString("actor_id");
        String year = request.getString("year");
        JSONArray array = new JSONArray();
        SqlRowSet rowSet = null;
        long start = System.currentTimeMillis();

        switch (type) {
            case IMPALA:
                result.put("Error", "Impala JDBC does not supported this query, please use Hue for this query.");
                return result;
//                rowSet = impalaTemplate.queryForRowSet(
//                        "SELECT sh.snapshot_id, " +
//                                "sh.title, " +
//                                "sh.release_date, " +
//                                "cd.category_name " +
//                                "FROM datedim AS dm " +
//                                "JOIN movietoactorbridge AS ab " +
//                                "ON dm.date_id = ab.date_id " +
//                                "JOIN amazonmoviesnapshot AS sh " +
//                                "ON sh.snapshot_id = ab.AmazonMovieSnapshot_snapshot_id " +
//                                "JOIN moviecategorybridge AS cb " +
//                                "ON cb.AmazonMovieSnapshot_snapshot_id = sh.snapshot_id " +
//                                "JOIN categorydim AS cd " +
//                                "ON cd.category_id = cb.CategoryDim_category_id " +
//                                "WHERE " +
//                                "dm.date_year = " + year + " " +
//                                "AND ab.ActorDim_actor_id = " + actorId
//                );
//                break;
            case MYSQL:
//                break;
            case MSSQLSERVER:
                rowSet = sqlServerTemplate.queryForRowSet(
                        "SELECT DISTINCT " +
                                "(sh.snapshot_id), " +
                                "sh.title, " +
                                "sh.release_date, " +
                                "ab.ActorDim_actor_id " +
                                "FROM " +
                                "AmazonMovieDW.MVMK.datedim AS dm " +
                                "JOIN AmazonMovieDW.MVMK.movietoactorbridge AS ab ON dm.date_id = ab.date_id " +
                                "JOIN AmazonMovieDW.MVMK.amazonmoviesnapshot AS sh ON sh.snapshot_id = ab.AmazonMovieSnapshot_snapshot_id " +
                                "WHERE " +
                                "dm.date_year = ? " +
                                "AND ab.ActorDim_actor_id = ?", year, actorId
                );
                break;
            case HIVE:
                break;
        }
        long end = System.currentTimeMillis();

        SqlRowSetMetaData metaData = rowSet.getMetaData();

        getData(array, rowSet, metaData);

        result.put("queryTime", (end - start) + "ms");
        result.put("movies", array);

        return result;
    }

    public JSONObject listMovieByDirectorAndActor(JSONObject request, DataBaseType type) {
        JSONObject result = new JSONObject();

        String actorId = request.getString("actor_id");
        String directorId = request.getString("director_id");
        JSONArray array = new JSONArray();
        SqlRowSet rowSet = null;
        long start = System.currentTimeMillis();

        switch (type) {
            case IMPALA:
                rowSet = impalaTemplate.queryForRowSet(
                        "( " +
                                " SELECT " +
                                "  snapshot_id, " +
                                "  title, " +
                                "  ranking, " +
                                "  categories " +
                                " FROM " +
                                "  movietoactorbridge AS ab " +
                                " JOIN movietodirectorbridge AS db ON ab.AmazonMovieSnapshot_snapshot_id = db.AmazonMovieSnapshot_snapshot_id " +
                                " JOIN amazonmoviesnapshot AS sh ON ab.AmazonMovieSnapshot_snapshot_id = sh.snapshot_id " +
                                " WHERE " +
                                "  ab.ActorDim_actor_id =  " + actorId +
                                " AND db.DirectorDim_director_id =  " + directorId +
                                ") " +
                                "UNION " +
                                "( " +
                                " SELECT " +
                                "  snapshot_id, " +
                                "  title, " +
                                "  ranking, " +
                                "  categories " +
                                " FROM " +
                                "  movietosupportingactorbridge AS sab " +
                                " JOIN movietodirectorbridge AS db ON sab.AmazonMovieSnapshot_snapshot_id = db.AmazonMovieSnapshot_snapshot_id " +
                                " JOIN amazonmoviesnapshot AS sh ON sab.AmazonMovieSnapshot_snapshot_id = sh.snapshot_id " +
                                " WHERE " +
                                "  sab.ActorDim_actor_id = " + actorId +
                                " AND db.DirectorDim_director_id = " + directorId +
                                ")"
                );
                break;
            case MYSQL:
                break;
            case MSSQLSERVER:
                rowSet = sqlServerTemplate.queryForRowSet(
                        "( " +
                                " SELECT " +
                                "  snapshot_id, " +
                                "  title, " +
                                "  ranking, " +
                                "  categories " +
                                " FROM " +
                                "  AmazonMovieDW.MVMK.movietoactorbridge AS ab " +
                                " JOIN AmazonMovieDW.MVMK.movietodirectorbridge AS db ON ab.AmazonMovieSnapshot_snapshot_id = db.AmazonMovieSnapshot_snapshot_id " +
                                " JOIN AmazonMovieDW.MVMK.amazonmoviesnapshot AS sh ON ab.AmazonMovieSnapshot_snapshot_id = sh.snapshot_id " +
                                " WHERE " +
                                "  ab.ActorDim_actor_id = ? " +
                                " AND db.DirectorDim_director_id = ? " +
                                ") " +
                                "UNION " +
                                "( " +
                                " SELECT " +
                                "  snapshot_id, " +
                                "  title, " +
                                "  ranking, " +
                                "  categories " +
                                " FROM " +
                                "  AmazonMovieDW.MVMK.movietosupportingactorbridge AS sab " +
                                " JOIN AmazonMovieDW.MVMK.movietodirectorbridge AS db ON sab.AmazonMovieSnapshot_snapshot_id = db.AmazonMovieSnapshot_snapshot_id " +
                                " JOIN AmazonMovieDW.MVMK.amazonmoviesnapshot AS sh ON sab.AmazonMovieSnapshot_snapshot_id = sh.snapshot_id " +
                                " WHERE " +
                                "  sab.ActorDim_actor_id = ? " +
                                " AND db.DirectorDim_director_id = ? " +
                                ") ", actorId, directorId, actorId, directorId
                );
                break;
            case HIVE:
                break;
        }
        long end = System.currentTimeMillis();

        SqlRowSetMetaData metaData = rowSet.getMetaData();

        getData(array, rowSet, metaData);

        result.put("queryTime", (end - start) + "ms");
        result.put("movies", array);

        return result;
    }

    public JSONObject listMovieByYearInSummerOrderByRank(JSONObject request, DataBaseType type) {
        JSONObject result = new JSONObject();
        String year = request.getString("year");
        JSONArray array = new JSONArray();
        SqlRowSet rowSet = null;
        long start = System.currentTimeMillis();

        switch (type) {
            case IMPALA:
                result.put("Error", "Impala JDBC does not supported this query, please use Hue for this query.");
                return result;
//                rowSet = impalaTemplate.queryForRowSet(
//                        "SELECT DISTINCT  " +
//                                " (sh.snapshot_id),  " +
//                                " sh.title,  " +
//                                " sh.ranking  " +
//                                "FROM datedim AS dd  " +
//                                "JOIN amazonmoviesnapshot AS sh "+
//                                " WHERE  " +
//                                "  dd.date_month = 6  " +
//                                "  OR dd.date_month = 7  " +
//                                "  OR dd.date_month = 8  " +
//                                "   AND dd.date_year = 2001 "+
//                                "AND sh.ranking IS NOT NULL  " +
//                                "AND sh.ranking <> (-1)  " +
//                                "ORDER BY  " +
//                                " sh.ranking"
//                );
//                break;
            case MYSQL:
//                break;
            case MSSQLSERVER:
                if (year.equals("0")) {
                    rowSet = sqlServerTemplate.queryForRowSet(
                            "SELECT COUNT(DISTINCT(sh.snapshot_id))\n" +
                                    "FROM\n" +
                                    "\tAmazonMovieDW.MVMK.datedim AS dd\n" +
                                    "JOIN AmazonMovieDW.MVMK.amazonmoviesnapshot AS sh \n" +
                                    "ON dd.date_id = sh.date_id\n" +
                                    "WHERE\n" +
                                    "\t(\n" +
                                    "\t\tdd.date_month = 6\n" +
                                    "\t\tOR dd.date_month = 7\n" +
                                    "\t\tOR dd.date_month = 8\n" +
                                    "\t)\n" +
                                    "AND sh.ranking IS NOT NULL\n" +
                                    "AND sh.ranking <>- 1"
                    );

                    rowSet.next();
                    result.put("count", rowSet.getInt(1));

                    rowSet = sqlServerTemplate.queryForRowSet(
                            "SELECT DISTINCT TOP 100 " +
                                    " (sh.snapshot_id),  " +
                                    " sh.title,  " +
                                    " sh.ranking  " +
                                    "FROM  " +
                                    " AmazonMovieDW.MVMK.datedim AS dd  " +
                                    "JOIN AmazonMovieDW.MVMK.amazonmoviesnapshot AS sh " +
                                    "ON dd.date_id = sh.date_id " +
                                    "WHERE  " +
                                    " (  " +
                                    "  dd.date_month = 6  " +
                                    "  OR dd.date_month = 7  " +
                                    "  OR dd.date_month = 8  " +
                                    " )  " +
                                    "AND sh.ranking IS NOT NULL  " +
                                    "AND sh.ranking <> -1  " +
                                    "ORDER BY  " +
                                    " sh.ranking  "
                    );

                }
                else {
                    rowSet = sqlServerTemplate.queryForRowSet(
                            "SELECT COUNT(DISTINCT(sh.snapshot_id))\n" +
                                    "FROM\n" +
                                    "\tAmazonMovieDW.MVMK.datedim AS dd\n" +
                                    "JOIN AmazonMovieDW.MVMK.amazonmoviesnapshot AS sh \n" +
                                    "ON dd.date_id = sh.date_id AND dd.date_year = ?\n" +
                                    "WHERE\n" +
                                    "\t(\n" +
                                    "\t\tdd.date_month = 6\n" +
                                    "\t\tOR dd.date_month = 7\n" +
                                    "\t\tOR dd.date_month = 8\n" +
                                    "\t)\n" +
                                    "AND sh.ranking IS NOT NULL\n" +
                                    "AND sh.ranking <>- 1", year
                    );

                    rowSet.next();
                    result.put("count", rowSet.getInt(1));

                    rowSet = sqlServerTemplate.queryForRowSet(
                            "SELECT DISTINCT TOP 100 " +
                                    " (sh.snapshot_id),  " +
                                    " sh.title,  " +
                                    " sh.ranking  " +
                                    "FROM  " +
                                    " AmazonMovieDW.MVMK.datedim AS dd  " +
                                    "JOIN AmazonMovieDW.MVMK.amazonmoviesnapshot AS sh " +
                                    "ON dd.date_year = ?  " +
                                    "AND dd.date_id = sh.date_id " +
                                    "WHERE  " +
                                    " (  " +
                                    "  dd.date_month = 6  " +
                                    "  OR dd.date_month = 7  " +
                                    "  OR dd.date_month = 8  " +
                                    " )  " +
                                    "AND sh.ranking IS NOT NULL  " +
                                    "AND sh.ranking <> -1  " +
                                    "ORDER BY  " +
                                    " sh.ranking  ", year
                    );
                }
                break;
            case HIVE:
                break;
        }
        long end = System.currentTimeMillis();

        SqlRowSetMetaData metaData = rowSet.getMetaData();

        getData(array, rowSet, metaData);

        result.put("queryTime", (end - start) + "ms");
        result.put("movies", array);

        return result;
    }

    public JSONObject getMovieByActOneByActTwo(JSONObject request, DataBaseType type) {
        JSONObject result = new JSONObject();

        String actor1Id = request.getString("actor_1_id");
        String actor2Id = request.getString("actor_2_id");
        JSONArray array = new JSONArray();
        SqlRowSet rowSet = null;
        long start = System.currentTimeMillis();

        switch (type) {
            case IMPALA:
                result.put("Error", "Impala JDBC does not supported this query, please use Hue for this query.");
                return result;
//                rowSet = impalaTemplate.queryForRowSet(
//                        "(  " +
//                                "  SELECT  " +
//                                "    snapshot_id,  " +
//                                "    title,  " +
//                                "    ranking  " +
//                                "  FROM  " +
//                                "    movietoactorbridge AS ab  " +
//                                "  JOIN movietosupportingactorbridge AS sab ON ab.AmazonMovieSnapshot_snapshot_id = sab.AmazonMovieSnapshot_snapshot_id  " +
//                                "  JOIN amazonmoviesnapshot AS sh ON ab.AmazonMovieSnapshot_snapshot_id = sh.snapshot_id  " +
//                                "  WHERE  " +
//                                "    ab.ActorDim_actor_id =  " + actor1Id +
//                                "  AND sab.ActorDim_actor_id = " + actor2Id +
//                                "  AND ranking IS NOT NULL  " +
//                                ")  " +
//                                "UNION  " +
//                                "  (  " +
//                                "    SELECT  " +
//                                "      snapshot_id,  " +
//                                "      title,  " +
//                                "      ranking  " +
//                                "    FROM  " +
//                                "      movietoactorbridge AS ab  " +
//                                "    JOIN movietosupportingactorbridge AS sab ON ab.AmazonMovieSnapshot_snapshot_id = sab.AmazonMovieSnapshot_snapshot_id  " +
//                                "    JOIN amazonmoviesnapshot AS sh ON ab.AmazonMovieSnapshot_snapshot_id = sh.snapshot_id  " +
//                                "    WHERE  " +
//                                "      ab.ActorDim_actor_id = " + actor2Id +
//                                "    AND sab.ActorDim_actor_id = " + actor1Id +
//                                "    AND ranking IS NOT NULL  " +
//                                "  )  " +
//                                "UNION  " +
//                                "  (  " +
//                                "    SELECT  " +
//                                "      snapshot_id,  " +
//                                "      title,  " +
//                                "      ranking  " +
//                                "    FROM  " +
//                                "      movietoactorbridge AS ab1  " +
//                                "    JOIN movietoactorbridge AS ab2 ON ab1.AmazonMovieSnapshot_snapshot_id = ab2.AmazonMovieSnapshot_snapshot_id  " +
//                                "    JOIN amazonmoviesnapshot AS sh ON ab1.AmazonMovieSnapshot_snapshot_id = sh.snapshot_id  " +
//                                "    WHERE  " +
//                                "      ab1.ActorDim_actor_id = " + actor1Id +
//                                "    AND ab2.ActorDim_actor_id = " + actor2Id +
//                                "    AND ranking IS NOT NULL  " +
//                                "  )  " +
//                                "UNION  " +
//                                "  (  " +
//                                "    SELECT  " +
//                                "      snapshot_id,  " +
//                                "      title,  " +
//                                "      ranking  " +
//                                "    FROM  " +
//                                "      movietosupportingactorbridge AS sab1  " +
//                                "    JOIN movietosupportingactorbridge AS sab2 ON sab1.AmazonMovieSnapshot_snapshot_id = sab2.AmazonMovieSnapshot_snapshot_id  " +
//                                "    JOIN amazonmoviesnapshot AS sh ON sab1.AmazonMovieSnapshot_snapshot_id = sh.snapshot_id  " +
//                                "    WHERE  " +
//                                "      sab1.ActorDim_actor_id = " + actor2Id +
//                                "    AND sab2.ActorDim_actor_id = " + actor1Id +
//                                "    AND ranking IS NOT NULL  " +
//                                "  )  " +
//                                "ORDER BY  " +
//                                "  ranking  "
//                );
            case MYSQL:
            case MSSQLSERVER:
                rowSet = sqlServerTemplate.queryForRowSet(
                        "(  " +
                                "  SELECT  " +
                                "    snapshot_id,  " +
                                "    title,  " +
                                "    ranking  " +
                                "  FROM  " +
                                "    AmazonMovieDW.MVMK.movietoactorbridge AS ab  " +
                                "  JOIN AmazonMovieDW.MVMK.movietosupportingactorbridge AS sab ON ab.AmazonMovieSnapshot_snapshot_id = sab.AmazonMovieSnapshot_snapshot_id  " +
                                "  JOIN AmazonMovieDW.MVMK.amazonmoviesnapshot AS sh ON ab.AmazonMovieSnapshot_snapshot_id = sh.snapshot_id  " +
                                "  WHERE  " +
                                "    ab.ActorDim_actor_id = ?  " +
                                "  AND sab.ActorDim_actor_id = ?  " +
                                "  AND ranking IS NOT NULL  " +
                                ")  " +
                                "UNION  " +
                                "  (  " +
                                "    SELECT  " +
                                "      snapshot_id,  " +
                                "      title,  " +
                                "      ranking  " +
                                "    FROM  " +
                                "      AmazonMovieDW.MVMK.movietoactorbridge AS ab  " +
                                "    JOIN AmazonMovieDW.MVMK.movietosupportingactorbridge AS sab ON ab.AmazonMovieSnapshot_snapshot_id = sab.AmazonMovieSnapshot_snapshot_id  " +
                                "    JOIN AmazonMovieDW.MVMK.amazonmoviesnapshot AS sh ON ab.AmazonMovieSnapshot_snapshot_id = sh.snapshot_id  " +
                                "    WHERE  " +
                                "      ab.ActorDim_actor_id = ?  " +
                                "    AND sab.ActorDim_actor_id = ?  " +
                                "    AND ranking IS NOT NULL  " +
                                "  )  " +
                                "UNION  " +
                                "  (  " +
                                "    SELECT  " +
                                "      snapshot_id,  " +
                                "      title,  " +
                                "      ranking  " +
                                "    FROM  " +
                                "      AmazonMovieDW.MVMK.movietoactorbridge AS ab1  " +
                                "    JOIN AmazonMovieDW.MVMK.movietoactorbridge AS ab2 ON ab1.AmazonMovieSnapshot_snapshot_id = ab2.AmazonMovieSnapshot_snapshot_id  " +
                                "    JOIN AmazonMovieDW.MVMK.amazonmoviesnapshot AS sh ON ab1.AmazonMovieSnapshot_snapshot_id = sh.snapshot_id  " +
                                "    WHERE  " +
                                "      ab1.ActorDim_actor_id = ?  " +
                                "    AND ab2.ActorDim_actor_id = ?  " +
                                "    AND ranking IS NOT NULL  " +
                                "  )  " +
                                "UNION  " +
                                "  (  " +
                                "    SELECT  " +
                                "      snapshot_id,  " +
                                "      title,  " +
                                "      ranking  " +
                                "    FROM  " +
                                "      AmazonMovieDW.MVMK.movietosupportingactorbridge AS sab1  " +
                                "    JOIN AmazonMovieDW.MVMK.movietosupportingactorbridge AS sab2 ON sab1.AmazonMovieSnapshot_snapshot_id = sab2.AmazonMovieSnapshot_snapshot_id  " +
                                "    JOIN AmazonMovieDW.MVMK.amazonmoviesnapshot AS sh ON sab1.AmazonMovieSnapshot_snapshot_id = sh.snapshot_id  " +
                                "    WHERE  " +
                                "      sab1.ActorDim_actor_id = ?  " +
                                "    AND sab2.ActorDim_actor_id = ?  " +
                                "    AND ranking IS NOT NULL  " +
                                "  )  " +
                                "ORDER BY  " +
                                "  ranking", actor1Id, actor2Id, actor2Id, actor1Id, actor1Id, actor2Id, actor2Id, actor1Id
                );
                break;
            case HIVE:
                break;
        }
        long end = System.currentTimeMillis();

        SqlRowSetMetaData metaData = rowSet.getMetaData();

        getData(array, rowSet, metaData);

        result.put("queryTime", (end - start) + "ms");
        result.put("movies", array);

        return result;
    }

    private void getData(JSONArray array, SqlRowSet rowSet, SqlRowSetMetaData metaData) {
        while (rowSet.next()) {
            JSONObject movie = new JSONObject();

            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String col = metaData.getColumnName(i);
                String val = rowSet.getString(col);
                movie.put(col, val);
            }

            array.add(movie);
        }
    }
}
