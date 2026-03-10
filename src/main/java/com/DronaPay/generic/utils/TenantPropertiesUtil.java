package com.DronaPay.generic.utils;

import java.io.IOException;
import java.util.Properties;



public class TenantPropertiesUtil {
    
    public static Properties getTenantProps(String tenantid) throws IOException {
        Properties props = new Properties();
        props.load(TenantPropertiesUtil.class.getClassLoader().getResourceAsStream("application.properties_" + tenantid));
        return props;
    }
}
