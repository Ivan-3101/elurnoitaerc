package com.DronaPay.frm.Onboarding;

import org.apache.ibatis.logging.LogFactory;
//import org.camunda.bpm.engine.test.ProcessEngineRule;
//import org.camunda.bpm.extension.process_test_coverage.junit.rules.TestCoverageProcessEngineRuleBuilder;
import org.cibseven.bpm.engine.test.ProcessEngineRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;

//import static org.camunda.bpm.engine.test.assertions.ProcessEngineTests.*;
import static org.cibseven.bpm.engine.test.assertions.ProcessEngineTests.*;

/**
 * Test case starting an in-memory database-backed Process Engine.
 */
public class ProcessUnitTest {

  static {
    LogFactory.useSlf4jLogging(); // MyBatis
  }

  @ClassRule
//  @Rule
//  public static ProcessEngineRule rule = TestCoverageProcessEngineRuleBuilder.create().build();
  public static ProcessEngineRule rule = new ProcessEngineRule();

  @Before
  public void setup() {
    init(rule.getProcessEngine());
  }

//  @Test
//  @Deployment(resources = {"process.bpmn"})
//  public void testHappyPath() {
//    // Drive the process by API and assert correct behavior by camunda-bpm-assert
//
//    ProcessInstance processInstance = processEngine().getRuntimeService()
//        .startProcessInstanceByKey(ProcessConstants.PROCESS_DEFINITION_KEY);
//
////    assertThat(processInstance).isEnded();
//  }

}
