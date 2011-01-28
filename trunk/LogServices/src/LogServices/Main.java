/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package LogServices;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;
import org.apache.log4j.Logger;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.io.*;
import java.util.*;
import java.sql.*;

/**
 *
 * @author FarmerLuo
 */
public class Main {

    public static int ExitFlag = 1;
    public static int Done = 0;
    private static Logger logger = Logger.getLogger(Main.class.getName());
    private static String path = null ;
    private static String ext = null;
    private static String host = null;
    private static String port = null;
    private static String database = null;
    private static String table = null;
    private static String username = null;
    private static String password = null;
    private static int dealy = 60;
    private static String emailTo = null;
    private static String emailFrom = null;
    private static String emailUser = null;
    private static String emailPasswd = null;
    private static String emailSmtp = null;
    private static boolean emailTLS = true;
    private static boolean emailSSL = false;
    private static int detectTime = 30;
    private static String[] emailToList = null;

    public static void main(String[] args) throws SQLException {
        Connection mysql_conn = null;
        long startTime = 0;
        long stopTime = 0;
        long dealyTime = 0;

        org.apache.log4j.PropertyConfigurator.configure("./log4j.properties");
        logger.info( "LogServices start ..." );

        loadConfig();

        doShutDownWork();

        try {
            logger.info( "Connecting to mysql host:" + host );
            mysql_conn = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + database + "?user=" + username + "&password=" + password + "&characterEncoding=UTF8");
        } catch (SQLException sQLException) {
            Done = 1;
            logger.error( sQLException );
        }
        
        Statement stmt = null;
        stmt = mysql_conn.createStatement();

        startTime = System.currentTimeMillis();

        while( ExitFlag == 1 ) {

            FileList findlist  = new FileList( path, ext );
            File [] ArrList = findlist.FileLists;

            Done = 0;
            for (int i = 0;i < ArrList.length; i++ ) {
                
                String sql = "LOAD DATA INFILE '" + ArrList[i].getAbsolutePath() + "' INTO TABLE " + table + " FIELDS TERMINATED BY ',';";

                logger.info(sql);
                try {
                    stmt.executeUpdate(sql);
                    boolean success = (new File( ArrList[i].getAbsolutePath() )).delete();
                    if (!success) {
                        logger.info( "Delete file " + ArrList[i].getAbsolutePath() + "failed" );
                    }
                    startTime = System.currentTimeMillis();
                } catch (SQLException sQLException) {
                    Done = 1;
                    logger.error( sQLException );
                }

                if ( ExitFlag == 0 ) {
                    logger.info( "Exit main inside loop." );
                    break;
                }
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

            //在超过detectTime时间内没有导入数据，则发警告邮件出去
            stopTime = System.currentTimeMillis();
            dealyTime = ( stopTime - startTime ) / 1000;

            if ( dealyTime >= detectTime ) {
                startTime = System.currentTimeMillis();
                logger.info( "no data import in " + detectTime + " second, warning mail sending." );
                for( int i=0; i<emailToList.length; i++ ) {
                    logger.info( "send mail to:" +  emailToList[i] );
                    sendMail(emailToList[i]);
                }
                logger.info( "warning mail send done." );
            }
            
        }
        
        stmt.close();
        mysql_conn.close();

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

        Properties p = new Properties();
        try {
            logger.info( "load configure file: config.properties " );
            p.load(new FileInputStream("config.properties"));
        } catch (IOException ex) {
            Done = 1;
            logger.error( ex );
        }

        path        = p.getProperty("path");
        ext         = p.getProperty("extend_name");
        host        = p.getProperty("host");
        port        = p.getProperty("port");
        database    = p.getProperty("database");
        table       = p.getProperty("table");
        username    = p.getProperty("username");
        password    = p.getProperty("password");
        dealy       = Integer.parseInt( p.getProperty("dealy") );
        emailTo     = p.getProperty("email_to");
        emailFrom   = p.getProperty("email_from");
        emailUser   = p.getProperty("email_user");
        emailPasswd = p.getProperty("email_passwd");
        emailSmtp   = p.getProperty("email_smtp");
        emailTLS    = Boolean.parseBoolean( p.getProperty("email_TLS") );
        emailSSL    = Boolean.parseBoolean( p.getProperty("email_SSL") );
        detectTime  = Integer.parseInt( p.getProperty("detect_time") );
        emailToList = emailTo.split(";"); 
        p = null;
        
        logger.info( "path = " + path );
        logger.info( "extend_name = " + ext );
        logger.info( "host = " + host );
        logger.info( "port = " + port );
        logger.info( "database = " + database );
        logger.info( "table = " + table );
        logger.info( "Username = " + username );
        logger.info( "password = " + password );
        logger.info( "dealy = " + dealy );
        logger.info( "email_to = " + emailTo );
        logger.info( "email_from = " + emailFrom );
        logger.info( "email_user = " + emailUser );
        logger.info( "email_passwd = " + emailPasswd );
        logger.info( "email_smtp = " + emailSmtp );
        logger.info( "email_TLS = " + emailTLS );
        logger.info( "email_SSL = " + emailSSL );
        logger.info( "detect_time = " + detectTime );
    }

    public static void sendMail( String emailAddr )
    {
        SimpleEmail email = new SimpleEmail();
        email.setTLS(emailTLS);
        email.setHostName(emailSmtp);
        email.setSSL(emailSSL);
        email.setAuthentication(emailUser, emailPasswd);

        Date now=new Date();
        SimpleDateFormat f=new SimpleDateFormat("yyyy年MM月dd日 kk点mm分");

        try
        {
            email.addTo(emailAddr);
            email.setFrom(emailFrom);
            email.setSubject("LogService未检测到日志文件");
            email.setCharset("GB2312");
            email.setMsg("警告：LogService在" + detectTime/60  + "分钟内未检测到日志文件!\n邮件发送时间："+ f.format(now));
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

}
