package io.debezium.connector.oracle;

import oracle.sql.NUMBER;
import oracle.streams.StreamsException;

/**
 * Fix bug for converting rawPosition(byte[]) to long value from XStream LCR getPosition()
 * If rawPosition is a large number, the original method will return a negative value which makes nonsense.Because according to oracle doc, scn should always
 * be a positive value.
 * An incorrect scn number will impact the debezium oracle connector behavior. See more details in DBZ-1354
 *
 * LcrPositionTest.java covered some test data, please check for more details
 *
 * @author Nick Gu
 */
public class OracleXStreamUtility {
    public static NUMBER getSCNFromPosition(byte[] rawPosition) throws StreamsException {
        long scn = 0L;

        if(rawPosition == null) {
            throw new StreamsException("XStream getSCNFromPosition: invalid position.");
        }

        //check rawPosition length and position version
        int length = rawPosition.length;
        byte positionVersion = rawPosition[rawPosition.length - 1];
        if(length != 29 && length != 33 || positionVersion == 1 && length != 29 || positionVersion == 2 && length != 33) {
            throw new StreamsException("XStream getSCNFromPosition: invalid position. " + length);
        }
        //calculate scn value based on different length
        byte startPosition;
        if(length == 29) {
            // length is equal to 29 use below
            long factor;
            startPosition = 14;

            factor = (long)(rawPosition[startPosition] & 255);
            factor <<= 40;
            scn += factor;

            factor = (long)(rawPosition[startPosition + 1] & 255);
            factor <<= 32;
            scn += factor;

            factor = (long)(rawPosition[startPosition + 2] & 255);
            factor <<= 24;
            scn += factor;


            factor = (long)(rawPosition[startPosition + 3] & 255);
            factor <<= 16;
            scn += factor;


            factor = (long)(rawPosition[startPosition + 4] & 255);
            factor <<= 8;
            scn += factor;

            factor = (long)(rawPosition[startPosition + 5] & 255);
            scn += factor;

        } else {
            // length is equal to 33 use below
            long factor;
            startPosition = 16;

            factor = (long)(rawPosition[startPosition] & 255);
            factor <<= 56;
            scn += factor;

            factor = (long)(rawPosition[startPosition + 1] & 255);
            factor <<= 48;
            scn += factor;

            factor = (long)(rawPosition[startPosition + 2] & 255);
            factor <<= 40;
            scn += factor;

            factor = (long)(rawPosition[startPosition + 3] & 255);
            factor <<= 32;
            scn += factor;

            factor = (long)(rawPosition[startPosition + 4] & 255);
            factor <<= 24;
            scn += factor;

            factor = (long)(rawPosition[startPosition + 5] & 255);
            factor <<= 16;
            scn += factor;

            factor = (long)(rawPosition[startPosition + 6] & 255);
            factor <<= 8;
            scn += factor;

            factor = (long)(rawPosition[startPosition + 7] & 255);
            scn += factor;
        }

        return new NUMBER(scn);
    }
}
