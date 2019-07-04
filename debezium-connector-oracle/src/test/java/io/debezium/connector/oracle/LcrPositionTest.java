package io.debezium.connector.oracle;

import io.debezium.doc.FixFor;
import oracle.sql.NUMBER;
import oracle.streams.StreamsException;
import oracle.streams.XStreamUtility;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import static org.fest.assertions.Assertions.assertThat;

/**
 * Unit test to test bug fix for DBZ-1354。
 * This is related to  oracle OracleXStreamUtility bugs.
 * Create a new method to fix the bug. Please see OracleXStreamUtility.java for more details
 *
 * @author Nick Gu
 */
@RunWith(Parameterized.class)
public class LcrPositionTest {
    private long expected;

    @Parameters
    public static Collection<Long> prepareData() {
        return Arrays.asList(140393875109L,62354L, 63238689L,7890876L, 280593865909L,765L, 581533896907L, 333333L, 73458923750974L, 86L);
    }

    public LcrPositionTest(long expected) {
        this.expected = expected;
    }

    @Test
    @FixFor("DBZ-1354")
    public void testLcrPositionGivingALargeScnNumber() throws StreamsException {
        long actual;

        //scn version 1 for oracle 11
        byte[] scnV1 = XStreamUtility.convertSCNToPosition(new NUMBER(expected), XStreamUtility.POS_VERSION_V1);
        LcrPosition lcrPositionV1 = new LcrPosition(scnV1);

        actual = lcrPositionV1.getScn();
        assertThat(actual).isEqualTo(expected);


        //scn version 2 for oracle 12+
        byte[] scnV2 = XStreamUtility.convertSCNToPosition(new NUMBER(expected), XStreamUtility.POS_VERSION_V2);
        LcrPosition lcrPositionV2 = new LcrPosition(scnV2);
        actual = lcrPositionV2.getScn();

        assertThat(actual).isEqualTo(expected);
    }

}
