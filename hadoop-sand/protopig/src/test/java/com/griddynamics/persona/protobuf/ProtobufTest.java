package com.griddynamics.persona.protobuf;

/**
 * vladvlaskin | 6/28/12/11:54 AM
 */
public class ProtobufTest {

    //Protobuf.UserStaticCell cell with Values:
    //ip: 86.149.6.156
    //browser: Firefox 13
    //platform: Computer
    //oS: Windows 7
    //browserType: Browser
    static String userStaticInfo = "\n\u000C86.149.6.156\u0012\nFirefox 13\u001A\u0008Computer\"\u0009Windows 7(\u00002\u0002J6B\u0002GBJ\u0007Browser";

    static String pageEventInfo = "\n%\n\u0012com.humorcouch.www\u001A\u000F\u0008\u00D9\u00C0\u0016\u0012\u0007160x600(\u0001";

//    @Test
//    public void testProtobufUsage() throws Exception {
//
//        HBaseData.UserStaticCell staticCell = HBaseData.UserStaticCell.parseFrom(userStaticInfo.getBytes());
//
//        assertEquals(staticCell.getBrowser(), "Firefox 13");
//        assertEquals(staticCell.getBrowserType(), "Browser");
//        assertEquals(staticCell.getPlatform(), "Computer");
//        assertEquals(staticCell.getOS(), "Windows 7");
//        assertEquals(staticCell.getIP(), "86.149.6.156");
//    }
//
//    @Test
//    public void testPageEventUsage() throws Exception {
//        //Protobuf.PageEventCell eventCell = Protobuf.PageEventCell.parseFrom(pageEventInfo.getBytes());
//        HBaseData.PageEventCell eventCell = HBaseData.PageEventCell.parseFrom(pageEventInfo.getBytes());
//        eventCell.getClass();
//
//        HBaseData.UserEventCell userEventCell = HBaseData.UserEventCell.parseFrom(pageEventInfo.getBytes());
//        userEventCell.getClass();
//
//        assertEquals(eventCell.getPageInfo(0).getAdCount(), 0);
//        assertEquals(eventCell.getPageInfo(0).getSerializedSize(), 37);
//        assertEquals(eventCell.getPageInfoCount(), 1);
//        assertEquals(eventCell.getPageInfoList().get(0).getUserId(), "com.humorcouch.www");
//    }

}
