package v57.channel.tests;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({
    AProcessParityTest.class,
    BWebSocketParityTest.class,
    CTsInteropTest.class,
})
public class AllTestsSuite {
}
