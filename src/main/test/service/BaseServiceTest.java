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
}