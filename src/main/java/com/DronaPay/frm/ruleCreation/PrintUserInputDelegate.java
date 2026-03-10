package com.DronaPay.frm.ruleCreation;

import lombok.extern.slf4j.Slf4j;
import org.cibseven.bpm.engine.delegate.DelegateExecution;
import org.cibseven.bpm.engine.delegate.JavaDelegate;

@Slf4j
public class PrintUserInputDelegate implements JavaDelegate {

    @Override
    public void execute(DelegateExecution execution) throws Exception {
        String userInput = (String) execution.getVariable("userInput");
        log.info("=== SubProcess Task - User Input Received ===");
        log.info("userInput: {}", userInput);
    }
}