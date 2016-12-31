package controller;

import controller.base.AbstractController;
import org.springframework.web.bind.annotation.RequestMapping;
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
@RequestMapping("/api/m")
public class MySQLController extends AbstractController {
    public MySQLController(BaseService service) {
        super(service);
        type = DataBaseType.MYSQL;
    }
}
