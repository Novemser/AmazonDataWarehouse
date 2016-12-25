package controller;

import com.alibaba.fastjson.JSONObject;
import org.springframework.web.bind.annotation.RequestBody;

/**
 * 基类Url映射
 * Project: DataWarehouseServer
 * Package: controller
 * Author:  Novemser
 * 2016/12/24
 */

public interface IBaseController {
    JSONObject getMovieCountByYear(@RequestBody JSONObject request);
    JSONObject getMovieCountByYearSeason(@RequestBody JSONObject request);
    JSONObject getMovieCountByYearMonth(@RequestBody JSONObject request);
    JSONObject getMovieCountByYearDay(@RequestBody JSONObject request);
    JSONObject getMovieCountByYearSeasonDay(@RequestBody JSONObject request);
    JSONObject getMovieCountByYearMonthDay(@RequestBody JSONObject request);
    JSONObject getDirectorById(@RequestBody JSONObject request);
    JSONObject getActorById(@RequestBody JSONObject request);
    JSONObject getMovieByCategoryId(@RequestBody JSONObject request);
    JSONObject getMoviesByNameLike(@RequestBody JSONObject request);
    JSONObject listAllMovieCountByYear();
}
