/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package LogServices;
import java.util.logging.Level;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;
import org.apache.log4j.Logger;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.io.*;
import java.util.*;
import java.sql.*;
import org.w3c.dom.*;
import javax.xml.parsers.*;
import org.xml.sax.SAXException;


/**
 *
 * @author FarmerLuo
 */
public class Main {

    public static int ExitFlag = 1;
    public static int Done = 0;
    private static Logger logger = Logger.getLogger(Main.class.getName());
    private static int dealy = 60;
    private static int detectTime = 30;
    private static String[] emailToList = null;
    private static Document doc = null;
    private static HashMap<String,String> config_opt = null;
    private static HashMap<String,String>[] config_sites = null;

    public static void main(String[] args) throws SQLException {
        Connection mysql_conn = null;
        long[] startTime;
        long[] stopTime;
        long[] dealyTime;

        org.apache.log4j.PropertyConfigurator.configure("./log4j.properties");
        logger.info( "LogServices start ..." );

        loadConfig();

        doShutDownWork();

        startTime = new long[config_sites.length];
        stopTime = new long[config_sites.length];
        dealyTime = new long[config_sites.length];

        while( ExitFlag == 1 ) {

            for ( int j = 0; j < config_sites.length; j++ ) {

                if ( startTime[j] == 0 )  startTime[j] = System.currentTimeMillis();
                
                //在超过detectTime时间内没有导入数据，则发警告邮件出去
                stopTime[j] = System.currentTimeMillis();
                dealyTime[j] = ( stopTime[j] - startTime[j] ) / 1000;

                if ( dealyTime[j] >= detectTime ) {
                    startTime[j] = System.currentTimeMillis();
                    logger.info( "no data import in " + detectTime + " second, warning mail sending." );
                    for( int i=0; i<emailToList.length; i++ ) {
                        logger.info( "send mail to:" +  emailToList[i] );
                        sendMail( emailToList[i], config_sites[j].get("name").toString() );
                    }
                    logger.info( "warning mail send done." );
                }
                
                FileList findlist  = new FileList( config_sites[j].get("path").toString(), config_sites[j].get("extend_name").toString());
                File [] ArrList = findlist.FileLists;
                
                //如果没有扫描到数据文件，则退出本次循环
                if ( ArrList.length == 0 ) continue;

                try {
                    logger.info( "connecting to mysql host:" + config_sites[j].get("host") + "database:" + config_sites[j].get("database") );
                    mysql_conn = DriverManager.getConnection("jdbc:mysql://" + config_sites[j].get("host") + ":"
                            + config_sites[j].get("port") + "/" + config_sites[j].get("database") + "?user=" +
                            config_sites[j].get("username") + "&password=" + config_sites[j].get("password") + "&characterEncoding=UTF8");

                } catch (SQLException sQLException) {
                    Done = 1;
                    logger.error( sQLException );
                }

                Statement stmt = null;
                stmt = mysql_conn.createStatement();

                Done = 0;
                for (int i = 0;i < ArrList.length; i++ ) {

                    String sql = "LOAD DATA INFILE '" + ArrList[i].getAbsolutePath() + "' INTO TABLE " + config_sites[j].get("table") + " FIELDS TERMINATED BY ',';";

                    logger.info(config_sites[j].get("name").toString() + ":" + sql);
                    try {
                        stmt.executeUpdate(sql);
                        boolean success = (new File( ArrList[i].getAbsolutePath() )).delete();
                        if (!success) {
                            logger.info( "Delete file " + ArrList[i].getAbsolutePath() + "failed" );
                        }
                        startTime[j] = System.currentTimeMillis();
                    } catch (SQLException sQLException) {
                        Done = 1;
                        logger.error( sQLException );
                    }

                    if ( ExitFlag == 0 ) {
                        logger.info( "Exit main inside loop." );
                        break;
                    }
                }
                stmt.close();
                mysql_conn.close();
                logger.info( "disconnect mysql host:" + config_sites[j].get("host") + "database:" + config_sites[j].get("database") );

            }

            Done = 1;

            //暂停dealy秒
            if ( ExitFlag == 1 ) {
                try {
                    logger.info( "Sleep Time:" + dealy + "s" );
                    Thread.sleep ( dealy * 1000 ) ;
                } catch (InterruptedException ie) {
                    Done = 1;
                    logger.error( ie );
                }
            }
            
        }
  
    }

    public static void destroyExit(){
        logger.info("Waiting for exit LogServices ...");
        ExitFlag = 0;
        while ( Done == 0 ) {
            try {
                Thread.sleep ( 1000 ) ;
            } catch (InterruptedException ie) {
                logger.error( ie );
            }
        }
        logger.info("LogServices exit done.");
    }

    public static void loadConfig(){

        config_opt  = new HashMap<String,String>();

        logger.info( "Loading config.xml...");
        viewXML("./config.xml");

        emailToList = config_opt.get("email_to").toString().split(";");
        dealy       = Integer.parseInt( config_opt.get("dealy").toString() );
        detectTime  = Integer.parseInt( config_opt.get("detect_time").toString() );

        Set<String> keys = config_opt.keySet();
        for(String key: keys) {
            logger.info( key + " == " + config_opt.get(key));
        }

        for ( int i = 0; i < config_sites.length; i++ ) {
            logger.info( "site " + i + ":");
            keys = config_sites[i].keySet();
            for(String key: keys) {
                logger.info( key + " == " + config_sites[i].get(key));
            }
        }
    }

    public static void sendMail( String emailAddr , String sitename )
    {
        SimpleEmail email = new SimpleEmail();
        email.setTLS(Boolean.parseBoolean(config_opt.get("emailTLS").toString()));
        email.setHostName(config_opt.get("emailSmtp").toString());
        email.setSSL(Boolean.parseBoolean(config_opt.get("emailSSL").toString()));
        email.setAuthentication(config_opt.get("emailUser").toString(), config_opt.get("emailPasswd").toString());

        Date now=new Date();
        SimpleDateFormat f=new SimpleDateFormat("yyyy年MM月dd日 kk点mm分");

        try
        {
            email.addTo(config_opt.get("emailAddr").toString());
            email.setFrom(config_opt.get("emailFrom").toString());
            email.setSubject("LogService未检测到日志文件");
            email.setCharset("GB2312");
            email.setMsg("Site:" + sitename + "\n警告：LogService在" + detectTime/60  + "分钟内未检测到日志文件!\n邮件发送时间："+ f.format(now));
            email.send();
        } catch (EmailException e) {
            logger.error(e);
        }
    }

    private static synchronized void doShutDownWork() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public synchronized void run() {
                destroyExit();
            }
        });

    }

    // 该方法负责把XML文件的内容显示出来
    public static void viewXML(String xmlFile) {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = null;

        try {
            db = dbf.newDocumentBuilder();
        } catch (ParserConfigurationException ex) {
            logger.error( ex );
        }

        try {
            doc = db.parse(new File(xmlFile));
        } catch (SAXException ex) {
            logger.error( ex );
        } catch (IOException ex) {
            logger.error( ex );
        }

        // 在xml文件里,只有一个根元素,先把根元素拿出来看看
        //Element element = doc.getDocumentElement();
        //System.out.println("根元素为:" + element.getTagName());
        // 在xml文件里,只有一个根元素,先把根元素拿出来看看
        //Element element = doc.getDocumentElement();
        //System.out.println("根元素为:" + element.getTagName());


        // 先取全局选项，存放在HashMap config_opt内
        NodeList nodeList = doc.getElementsByTagName("section");
        //System.out.println("section节点链的长度:" + nodeList.getLength());

        Node fatherNode = nodeList.item(0);
//        System.out.println("父节点为:" + fatherNode.getNodeName());
//
//        把父节点的属性拿出来
//        NamedNodeMap attributes = fatherNode.getAttributes();
//        for (int i = 0; i < attributes.getLength(); i++) {
//            Node attribute = attributes.item(i);
//            System.out.println("book的属性名为:" + attribute.getNodeName()
//            + " 相对应的属性值为:" + attribute.getNodeValue());
//        }

        NodeList childNodes = fatherNode.getChildNodes();
        //System.out.println(childNodes.getLength());
        for (int j = 0; j < childNodes.getLength(); j++) {
            Node childNode = childNodes.item(j);
            // 如果这个节点属于Element ,再进行取值
            if (childNode instanceof Element) {
                //System.out.println("子节点名为:" + childNode.getNodeName() + "" + "相对应的值为" + childNode.getFirstChild().getNodeValue());
                config_opt.put(childNode.getNodeName(), childNode.getFirstChild().getNodeValue());
            }
        }

        //再取sites的选择，存放在config_sites内
        NodeList nList = doc.getElementsByTagName("site");
        //System.out.println("site节点链的长度:" + nList.getLength());

        config_sites = new HashMap[nList.getLength()];
        for ( int i = 0; i< nList.getLength(); i++ ){
            Node fNode = nList.item(i);
            NodeList cNodes = fNode.getChildNodes();

            config_sites[i] = new HashMap<String,String>();
            for (int j = 0; j < cNodes.getLength(); j++) {
                Node cNode = cNodes.item(j);
                // 如果这个节点属于Element ,再进行取值
                if (cNode instanceof Element) {
                    //System.out.println("子节点名为:" + cNode.getNodeName() + "" + "相对应的值为" + cNode.getFirstChild().getNodeValue());
                    config_sites[i].put(cNode.getNodeName(), cNode.getFirstChild().getNodeValue());
                }
            }
        }

    }

}
