package controller.base;

import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import service.BaseService;
import util.DataBaseType;

/**
 * Project: DataWarehouseServer
 * Package: controller
 * Author:  Novemser
 * 2016/12/26
 */
@RestController
public class AbstractController implements IBaseController {
    protected DataBaseType type;

    private final BaseService service;

    @Autowired
    public AbstractController(BaseService service) {
        this.service = service;
    }

    @Override
    @PostMapping("/year")
    public JSONObject getMovieCountByYear(@RequestBody JSONObject request) {
        return service.getMovieCountByYear(request, type);
    }

    @Override
    @PostMapping("/year/season")
    public JSONObject getMovieCountByYearSeason(@RequestBody JSONObject request) {
        return service.getMovieCountByYearSeason(request, type);
    }

    @Override
    @PostMapping("/year/month")
    public JSONObject getMovieCountByYearMonth(@RequestBody JSONObject request) {
        return service.getMovieCountByYearMonth(request, type);
    }

    @Override
    @PostMapping("/year/day")
    public JSONObject getMovieCountByYearDay(@RequestBody JSONObject request) {
        return service.getMovieCountByYearDay(request, type);
    }

    @Override
    @PostMapping("/year/season/day")
    public JSONObject getMovieCountByYearSeasonDay(@RequestBody JSONObject request) {
        return service.getMovieCountByYearSeasonDay(request, type);
    }

    @Override
    @PostMapping("/year/month/day")
    public JSONObject getMovieCountByYearMonthDay(@RequestBody JSONObject request) {
        return service.getMovieCountByYearMonthDay(request, type);
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

    @Override
    @PostMapping("/movie/name/like")
    public JSONObject getMoviesByNameLike(@RequestBody JSONObject request) {
        return service.getMovieByNameLike(request, type);
    }

    @Override
    @PostMapping("/movie/all/year")
    public JSONObject listAllMovieCountByYear() {
        return service.listAllMovieCountByYear(type);
    }

    @Override
    @PostMapping("/movie/ranking/between")
    public JSONObject listMovieByRanking(@RequestBody JSONObject request) {
        return service.listMovieByRanking(request, type);
    }

    @Override
    @PostMapping("/movie/review/between")
    public JSONObject listMovieByReview(@RequestBody JSONObject request) {
        return service.listMovieByReview(request, type);
    }

    @Override
    @PostMapping("/movie/actor/id")
    public JSONObject getActorMovieById(@RequestBody JSONObject request) {
        return service.getActorMoviesById(request, type);
    }

    @Override
    @PostMapping("/movie/director/id")
    public JSONObject getDirectorMovieById(@RequestBody JSONObject request) {
        return service.getDirectorsMovieById(request, type);
    }

    @Override
    @PostMapping("/movie/category/id")
    public JSONObject getCategoryMovieById(@RequestBody JSONObject request) {
        return service.getMovieByCategoryId(request, type);
    }

    @Override
    @PostMapping("/movie/actor/id/year")
    public JSONObject getMovieByActorByYear(@RequestBody JSONObject request) {
        return service.listMovieByActorIdAndYear(request, type);
    }

    @Override
    @PostMapping("/movie/director/with/actor")
    public JSONObject getMovieByDirectorAndActor(@RequestBody JSONObject request) {
        return service.listMovieByDirectorAndActor(request, type);
    }

    @Override
    @PostMapping("/movie/summer/year")
    public JSONObject getMovieByYearInSummerOrderByRank(@RequestBody JSONObject request) {
        return service.listMovieByYearInSummerOrderByRank(request, type);
    }

    @Override
    @PostMapping("/movie/actors/id")
    public JSONObject getMovieByActOneByActTwo(@RequestBody JSONObject request) {
        return service.getMovieByActOneByActTwo(request, type);
    }
}
