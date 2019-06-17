
package io.debezium.connector.oracle.util;

import oracle.streams.LCR;
import oracle.streams.XStreamIn;
import oracle.streams.XStreamOut;

import java.sql.Connection;
import java.sql.DriverManager;

public class XStreamSimpleConnectionTest
{
    public static String xsoutName = "dbzxout";
    public static String out_url = null;
    public static Connection out_conn = null;
    public static XStreamIn xsIn = null;
    public static XStreamOut xsOut = null;
    public static byte[] lastPosition = null;
    public static byte[] processedLowPosition = null;

    /**
     * Simple applet to check XStream functionality
     * @param args
     */
    public static void main(String args[])
    {
        // get connection url to inbound and outbound server
        out_url = "jdbc:oracle:oci:@10.47.100.32:1521/orclcdb";

        // create connection to inbound and outbound server
        out_conn = createConnection(out_url, "c##xstrm", "xs");

        // attach to inbound and outbound server
        xsOut = attachOutbound(out_conn);

        // main loop to get lcrs
        get_lcrs(xsIn, xsOut);

        // detach from inbound and outbound server
        detachOutbound(xsOut);
    }

    // create a connection to an Oracle Database
    public static Connection createConnection(String url, String username, String passwd) {
        try {
            DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
            return DriverManager.getConnection(url, username, passwd);
        } catch(Exception e) {
            System.out.println("fail to establish DB connection to: " +url);
            e.printStackTrace();
            return null;
        }
    }

    // attach to the XStream Outbound Server
    public static XStreamOut attachOutbound(Connection out_conn) {
        XStreamOut xsOut;

        try {
            // when attach to an outbound server, client needs to tell outbound
            // server the last position.
            xsOut = XStreamOut.attach((oracle.jdbc.internal.OracleConnection)out_conn, xsoutName,
                    lastPosition, XStreamOut.DEFAULT_MODE);
            System.out.println("Attached to outbound server:"+xsoutName);
            System.out.print("Last Position is: ");
            if (lastPosition != null) {
                printHex(lastPosition);
            } else {
                System.out.println("NULL");
            }
            return xsOut;
        } catch(Exception e)  {
            System.out.println("cannot attach to outbound server: "+xsoutName);
            System.out.println(e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    // detach from the XStream Outbound Server
    public static void detachOutbound(XStreamOut xsOut)
    {
        try
        {
            xsOut.detach(XStreamOut.DEFAULT_MODE);
        }
        catch(Exception e)
        {
            System.out.println("cannot detach from the outbound server: "+xsoutName);
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    public static void get_lcrs(XStreamIn xsIn, XStreamOut xsOut) {

        if (null == xsOut) {
            System.out.println("xstreamOut is null");
            System.exit(0);
        }

        try {
            while(true) {
                // receive an LCR from outbound server
                LCR alcr = xsOut.receiveLCR(XStreamOut.DEFAULT_MODE);
                if (alcr != null && alcr.getCommandType()!=null && !alcr.getCommandType().isEmpty()) {
                    System.out.println("operation: " + alcr.getCommandType() + " transaction ID: " + alcr.getTransactionId());
                }
                if (xsOut.getBatchStatus() == XStreamOut.EXECUTING) // batch is active
                {
                    assert alcr != null;
                    // send the LCR to the inbound server
                    //                    xsIn.sendLCR(alcr, XStreamIn.DEFAULT_MODE);

                    processedLowPosition = alcr.getPosition();
                }
                else  // batch is end
                {
                    assert alcr == null;
                    // flush the network
                    //xsIn.flush(XStreamIn.DEFAULT_MODE);
                    // get the processed_low_position from inbound server
                    //processedLowPosition = xsIn.getProcessedLowWatermark();
                    // update the processed_low_position at oubound server
                    if (null != processedLowPosition)
                        xsOut.setProcessedLowWatermark(processedLowPosition,
                                XStreamOut.DEFAULT_MODE);
                }
            }
        }
        catch(Exception e)
        {
            System.out.println("exception when processing LCRs");
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    public static void printHex(byte[] b)
    {
        for (int i = 0; i < b.length; ++i)
        {
            System.out.print(
                    Integer.toHexString((b[i]&0xFF) | 0x100).substring(1,3));
        }
        System.out.println("");
    }
}