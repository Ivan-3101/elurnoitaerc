// src/main/java/com/DronaPay/frm/ruleCreation/HelloWorldDelegate.java
package com.DronaPay.frm.ruleCreation;

import lombok.extern.slf4j.Slf4j;
import org.cibseven.bpm.engine.delegate.DelegateExecution;
import org.cibseven.bpm.engine.delegate.JavaDelegate;

@Slf4j
public class HelloWorldDelegate implements JavaDelegate {
    @Override
    public void execute(DelegateExecution execution) throws Exception {
        log.info("=== Hello World from RuleCreation workflow! ===");
        log.info("Tenant: {}, BusinessKey: {}", execution.getTenantId(), execution.getBusinessKey());
        execution.setVariable("helloWorldResult", "success");
    }
}