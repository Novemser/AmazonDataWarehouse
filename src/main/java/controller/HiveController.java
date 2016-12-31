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
@RequestMapping("/api/h")
public class HiveController extends AbstractController {
    public HiveController(BaseService service) {
        super(service);
        type = DataBaseType.HIVE;
    }
}
