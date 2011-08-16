/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package LogServices;
import java.util.*;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;
import java.util.Date;
import java.text.SimpleDateFormat;

/**
 *
 * @author FarmerLuo
 */
public class CheckTimeout extends Thread {

    public LoadData load_data;
    private HashMap<String,String> config_opt = null;
    private HashMap<String,String>[] config_sites = null;
    public java.sql.Connection conn = null;
    private long[] startTime;
    private long[] stopTime;
    private long[] dealyTime;
    private static int detectTime = 30;
    public String[] emailToList = null;

    CheckTimeout(LoadData load_data_name) {
        load_data = load_data_name;
        config_sites = load_data.getconfig_sites();
        config_opt = load_data.getconfig_opt();
        detectTime  = Integer.parseInt( config_opt.get("detect_time").toString() );
        emailToList = config_opt.get("email_to").toString().split(";");
        this.setName("CheckTimeout");
    }

    @Override
    public void run(){

        stopTime = new long[config_sites.length];
        dealyTime = new long[config_sites.length];

        while ( true ) {
            for ( int j = 0; j < config_sites.length; j++ ) {
                if ( Boolean.parseBoolean(config_sites[j].get("send_mail").toString()) ) {
                    //在超过detectTime时间内没有导入数据，则发警告邮件出去
                    startTime = load_data.getstartTime();
                    stopTime[j] = System.currentTimeMillis();
                    dealyTime[j] = ( stopTime[j] - startTime[j] ) / 1000;

                    if ( dealyTime[j] >= detectTime ) {
                        load_data.setstartTime(j,System.currentTimeMillis() );
                        Main.logger.info( "no data import in " + detectTime + " second, warning mail sending." );
                        for( int i=0; i<emailToList.length; i++ ) {
                            Main.logger.info( "send mail to:" +  emailToList[i] );
                            sendMail( emailToList[i], config_sites[j].get("name").toString() );
                        }
                        Main.logger.info( "warning mail send done." );
                    }
                }

                try {
                    Thread.sleep(10000);
                } catch (InterruptedException ex) {
                    Main.logger.error( ex );
                }

            }
        }

    }

    public void sendMail( String emailAddr , String sitename )
    {
        SimpleEmail email = new SimpleEmail();
        email.setTLS(Boolean.parseBoolean(config_opt.get("email_TLS").toString()));
        email.setHostName(config_opt.get("email_smtp").toString());
        email.setSSL(Boolean.parseBoolean(config_opt.get("email_SSL").toString()));
        email.setAuthentication(config_opt.get("email_user").toString(), config_opt.get("email_passwd").toString());

        Date now=new Date();
        SimpleDateFormat f=new SimpleDateFormat("yyyy年MM月dd日 kk点mm分");

        try
        {
            email.addTo(emailAddr);
            email.setFrom(config_opt.get("email_from").toString());
            email.setSubject("LogService未检测到日志文件");
            email.setCharset("GB2312");
            email.setMsg("Site:" + sitename + "\n警告：LogService在" + detectTime/60  + "分钟内未检测到日志文件!\n邮件发送时间："+ f.format(now));
            email.send();
        } catch (EmailException e) {
            Main.logger.error(e);
        }
    }

}
