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
 * 2016/12/25
 */
@RestController
@RequestMapping("/api/i")
public class ImpalaController extends AbstractController {
    public ImpalaController(BaseService service) {
        super(service);
        type = DataBaseType.IMPALA;
    }
}
