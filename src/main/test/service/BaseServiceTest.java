package service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import util.DataBaseType;

/**
 * Project: DataWarehouseServer
 * Package: service
 * Author:  Novemser
 * 2016/12/24
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration({"file:src/main/webapp/WEB-INF/datawarehouse-dispatcher.xml"})
public class BaseServiceTest {
    @Autowired
    private BaseService service;

    @Test
    public void getMovieCountByYear() throws Exception {
        JSONObject object = new JSONObject();
        JSONObject result = null;
        object.put("year", "2001");
        result = service.getMovieCountByYear(object, DataBaseType.MSSQLSERVER);
        System.out.println(result.toJSONString());
        result = service.getMovieCountByYear(object, DataBaseType.IMPALA);
        System.out.println(result.toJSONString());
    }

    @Test
    public void getMovieCountBySeasonYear() throws Exception {
        JSONObject object = new JSONObject();
        object.put("year", 0);
        JSONArray array = new JSONArray();
        array.add(true);
        array.add(true);
        array.add(true);
        array.add(true);
        object.put("season", array);
        JSONObject result = service.getMovieCountByYearSeason(object, DataBaseType.MSSQLSERVER);
        System.out.println(result.toJSONString());
        result = service.getMovieCountByYearSeason(object, DataBaseType.IMPALA);
        System.out.println(result.toJSONString());
    }

    @Test
    public void getMovieCountByYearMonth() throws Exception {
        JSONObject object = new JSONObject();
        object.put("year", 2001);
        JSONArray array = new JSONArray();
        array.add(true);
        array.add(true);
        array.add(false);
        array.add(true);
        array.add(true);
        array.add(true);
        array.add(true);
        array.add(true);
        array.add(true);
        array.add(true);
        array.add(false);
        array.add(true);
        object.put("month", array);
        JSONObject result = service.getMovieCountByYearMonth(object, DataBaseType.MSSQLSERVER);
        System.out.println(result.toJSONString());
        result = service.getMovieCountByYearMonth(object, DataBaseType.IMPALA);
        System.out.println(result.toJSONString());
    }

    @Test
    public void getMovieCountByYearSeasonDay() throws Exception {
        JSONObject object = new JSONObject();
        object.put("year", 2001);
        JSONArray array = new JSONArray();
        array.add(true);
        array.add(true);
        array.add(true);
        array.add(false);
        array.add(true);
        array.add(true);
        array.add(true);
        object.put("day", array);
        array = new JSONArray();
        array.add(true);
        array.add(true);
        array.add(true);
        array.add(true);
        object.put("season", array);
        JSONObject result = service.getMovieCountByYearSeasonDay(object, DataBaseType.MSSQLSERVER);
        System.out.println(result.toJSONString());
        result = service.getMovieCountByYearSeasonDay(object, DataBaseType.IMPALA);
        System.out.println(result.toJSONString());
    }

    @Test
    public void getMovieCountByYearMonthDay() throws Exception {
        JSONObject object = new JSONObject();
        object.put("year", 2001);
        JSONArray array = new JSONArray();
        array.add(true);
        array.add(true);
        array.add(true);
        array.add(false);
        array.add(true);
        array.add(true);
        array.add(true);
        object.put("day", array);
        array = new JSONArray();
        array.add(true);
        array.add(true);
        array.add(true);
        array.add(true);
        array.add(true);
        array.add(true);
        array.add(true);
        array.add(true);
        array.add(true);
        array.add(true);
        array.add(true);
        array.add(true);

        object.put("month", array);
        JSONObject result = service.getMovieCountByYearMonthDay(object, DataBaseType.MSSQLSERVER);
        System.out.println(result.toJSONString());
        result = service.getMovieCountByYearMonthDay(object, DataBaseType.IMPALA);
        System.out.println(result.toJSONString());

    }

    @Test
    public void getMovieByNameLike() throws Exception {
        JSONObject object = new JSONObject();
        object.put("name", "picasso");
        JSONObject result = service.getMovieByNameLike(object, DataBaseType.MSSQLSERVER);
        System.out.println(result.toJSONString());
        result = service.getMovieByNameLike(object, DataBaseType.IMPALA);
        System.out.println(result.toJSONString());

    }

    @Test
    public void listAllMovies() throws Exception {
        JSONObject res = service.listAllMovieCountByYear(DataBaseType.MSSQLSERVER);
        System.out.println(res.toJSONString());
    }

    @Test
    public void listByRanking() throws Exception {
        JSONObject object = new JSONObject();
        object.put("min", 22);
        object.put("max", 10000);
        JSONObject res = service.listMovieByRanking(object, DataBaseType.MSSQLSERVER);
        System.out.println(res.toJSONString());
        res = service.listMovieByRanking(object, DataBaseType.IMPALA);
        System.out.println(res);
    }

    @Test
    public void listByReview() throws Exception {
        JSONObject object = new JSONObject();
        object.put("min", 10);
        object.put("max", 50);
        JSONObject res = service.listMovieByReview(object, DataBaseType.MSSQLSERVER);
        System.out.println(res.toJSONString());
        res = service.listMovieByReview(object, DataBaseType.IMPALA);
        System.out.println(res);
    }

    @Test
    public void getActorById() throws Exception {
        JSONObject object = new JSONObject();
        object.put("staring", true);
        object.put("actor_id", 11);
        object.put("supporting", true);
        JSONObject res = service.getActorMoviesById(object, DataBaseType.MSSQLSERVER);
        System.out.println(res.toJSONString());
        res = service.getActorMoviesById(object, DataBaseType.IMPALA);
        System.out.println(res);
    }

    @Test
    public void getDirectorsById() throws Exception {
        JSONObject object = new JSONObject();
        object.put("director_id", 11);
        JSONObject res = service.getDirectorsMovieById(object, DataBaseType.MSSQLSERVER);
        System.out.println(res.toJSONString());
        res = service.getDirectorsMovieById(object, DataBaseType.IMPALA);
        System.out.println(res);

    }

    @Test
    public void getMovieByCategoryId() throws Exception {
        JSONObject object = new JSONObject();
        object.put("category_id", 1);
        JSONObject res = service.getMovieByCategoryId(object, DataBaseType.MSSQLSERVER);
        System.out.println(res.toJSONString());
        res = service.getMovieByCategoryId(object, DataBaseType.IMPALA);
        System.out.println(res);

    }

    @Test
    public void listMovieByActorIdAndYear() throws Exception {
        JSONObject object = new JSONObject();
        object.put("actor_id", 17);
        object.put("year", "2001");
        JSONObject res = service.listMovieByActorIdAndYear(object, DataBaseType.MSSQLSERVER);
        System.out.println(res.toJSONString());
        res = service.listMovieByActorIdAndYear(object, DataBaseType.IMPALA);
        System.out.println(res);

    }

    @Test
    public void listMovieByDirectorAndActor() throws Exception {
        JSONObject object = new JSONObject();
        object.put("actor_id", 1);
        object.put("director_id", 1);
        JSONObject res = service.listMovieByDirectorAndActor(object, DataBaseType.MSSQLSERVER);
        System.out.println(res.toJSONString());
        res = service.listMovieByDirectorAndActor(object, DataBaseType.IMPALA);
        System.out.println(res);

    }

    @Test
    public void listMovieByYearInSummerOrderByRank() throws Exception {
        JSONObject object = new JSONObject();
        object.put("year", 0);
        JSONObject res = service.listMovieByYearInSummerOrderByRank(object, DataBaseType.MSSQLSERVER);
        System.out.println(res.toJSONString());
//        res = service.listMovieByYearInSummerOrderByRank(object, DataBaseType.IMPALA);
//        System.out.println(res);

    }

    @Test
    public void getMovieByActOneByActTwo() throws Exception {
        JSONObject object = new JSONObject();
        object.put("actor_1_id", "2506");
        object.put("actor_2_id", "43091");
        JSONObject res = service.getMovieByActOneByActTwo(object, DataBaseType.MSSQLSERVER);
        System.out.println(res.toJSONString());
        res = service.getMovieByActOneByActTwo(object, DataBaseType.IMPALA);
        System.out.println(res);
    }
}