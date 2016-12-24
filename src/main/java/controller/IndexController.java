package controller;

import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Project: DataWarehouseServer
 * Package: controller
 * Author:  Novemser
 * 2016/12/23
 */
@RestController
public class IndexController {
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

    @GetMapping("/index")
    public JSONObject test() {
        JSONObject object = new JSONObject();
        long begin, end;

        begin = System.currentTimeMillis();
        SqlRowSet sqlRowSet = sqlServerTemplate.queryForRowSet(
                "SELECT count(*) FROM AmazonMovieDW.db_owner.amazonmoviesnapshot"
        );
        end = System.currentTimeMillis();

        if (sqlRowSet.next()) {
//            object.put("sqlServer", sqlRowSet.getInt(1));
            object.put("sqlServerQTime", end - begin);
        }

        begin = System.currentTimeMillis();
        sqlRowSet = impalaTemplate.queryForRowSet(
                "SELECT count(*) FROM amazonmoviesnapshot"
        );
        end = System.currentTimeMillis();

        if (sqlRowSet.next()) {
//            object.put("impala", sqlRowSet.getInt(1));
            object.put("impalaQTime", end - begin);
        }

        begin = System.currentTimeMillis();
        sqlRowSet = mySqlTemplate.queryForRowSet(
                "SELECT count(*) FROM amazonmoviesnapshot"
        );
        end = System.currentTimeMillis();

        if (sqlRowSet.next()) {
//            object.put("mySql", sqlRowSet.getInt(1));
            object.put("mySqlQTime", end - begin);
        }

        begin = System.currentTimeMillis();
        sqlRowSet = hiveTemplate.queryForRowSet(
                "SELECT count(*) FROM amazonmoviesnapshot"
        );
        end = System.currentTimeMillis();

        if (sqlRowSet.next()) {
            object.put("hiveQTime", end - begin);
        }

        return object;
    }
}
