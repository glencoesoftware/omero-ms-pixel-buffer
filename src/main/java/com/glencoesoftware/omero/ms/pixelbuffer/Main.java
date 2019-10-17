package com.glencoesoftware.omero.ms.pixelbuffer;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import ome.io.nio.PixelsService;

public class Main {

    public static void main(String[] args) {

        //System.setProperty("omero.data.dir", "/foobar");

        ApplicationContext context = new ClassPathXmlApplicationContext(
                "classpath:ome/config.xml",
                "classpath:ome/services/datalayer.xml",
                "classpath*:beanRefContext.xml");
        PixelsService bean = (PixelsService) context.getBean("/OMERO/Pixels");
        System.err.println("Bean: " + bean);
    }

}
