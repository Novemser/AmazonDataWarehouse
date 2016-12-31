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
 * 2016/12/24
 */
@RestController
@RequestMapping("/api/s")
public class SqlServerController extends AbstractController {
    public SqlServerController(BaseService service) {
        super(service);
        type = DataBaseType.MSSQLSERVER;
    }
}
