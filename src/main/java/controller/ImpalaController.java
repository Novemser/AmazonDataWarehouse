package controller;

import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import service.BaseService;
import util.DataBaseType;

/**
 * Project: DataWarehouseServer
 * Package: controller
 * Author:  Novemser
 * 2016/12/25
 */
@RestController
@RequestMapping("/api/i")
public class ImpalaController implements IBaseController {
    private final BaseService service;

    @Autowired
    public ImpalaController(BaseService service) {
        this.service = service;
    }
    @Override
    @PostMapping("/year")
    public JSONObject getMovieCountByYear(@RequestBody JSONObject request) {
        return service.getMovieCountByYear(request, DataBaseType.IMPALA);
    }

    @Override
    @PostMapping("/year/season")
    public JSONObject getMovieCountByYearSeason(@RequestBody JSONObject request) {
        return service.getMovieCountByYearSeason(request, DataBaseType.IMPALA);
    }

    @Override
    @PostMapping("/year/month")
    public JSONObject getMovieCountByYearMonth(@RequestBody JSONObject request) {
        return service.getMovieCountByYearMonth(request, DataBaseType.IMPALA);
    }

    @Override
    @PostMapping("/year/day")
    public JSONObject getMovieCountByYearDay(@RequestBody JSONObject request) {
        return service.getMovieCountByYearDay(request, DataBaseType.IMPALA);
    }

    @Override
    @PostMapping("/year/season/day")
    public JSONObject getMovieCountByYearSeasonDay(@RequestBody JSONObject request) {
        return service.getMovieCountByYearSeasonDay(request, DataBaseType.IMPALA);
    }

    @Override
    @PostMapping("/year/month/day")
    public JSONObject getMovieCountByYearMonthDay(@RequestBody JSONObject request) {
        return service.getMovieCountByYearMonthDay(request, DataBaseType.IMPALA);
    }

    @Override
    @PostMapping("/director")
    public JSONObject getDirectorById(@RequestBody JSONObject request) {
        return null;
    }

    @Override
    @PostMapping("/actor")
    public JSONObject getActorById(@RequestBody JSONObject request) {
        return null;
    }

    @Override
    @PostMapping("/category")
    public JSONObject getMovieByCategoryId(@RequestBody JSONObject request) {
        return null;
    }
}
