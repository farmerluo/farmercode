/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package LogServices;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.io.*;
import java.util.*;
import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.w3c.dom.*;
import javax.xml.parsers.*;
import org.xml.sax.SAXException;
import com.mysql.jdbc.*;
import com.vertica.*;
import java.io.*;
import java.util.zip.GZIPInputStream;

/**
 *
 * @author FarmerLuo
 */
public class LoadData  extends Thread {
    private static int dealy = 60;
    private static Document doc = null;
    private HashMap<String,String> config_opt = null;
    private HashMap<String,String>[] config_sites = null;
    public java.sql.Connection conn = null;
    private long[] startTime;
    private boolean flag = true;

    @Override
    public void run(){
        
        this.setName("LoadData");

        try {
            executeLoad();
        } catch (SQLException ex) {
            Main.logger.error( ex );
        }

    }

    public synchronized long[] getstartTime() {
        return startTime;
    }

    public synchronized HashMap<String,String>[] getconfig_sites() {
        return config_sites;
    }

    public synchronized HashMap<String,String> getconfig_opt() {
        return config_opt;
    }

    public synchronized void setstartTime( int j, long newtime ) {
        startTime[j] = newtime;
    }

   public synchronized void setflag( boolean newflag ) {
        flag = newflag;
    }
   

   public int unzip(String srcfile, String dstfile) {
        FileInputStream fin;
        
        try {
            fin = new FileInputStream(srcfile);
            FileOutputStream out = new FileOutputStream(dstfile);
            GZIPInputStream gzIn = new GZIPInputStream(fin);
            int buffersize = 1024;
            byte[] buffer = new byte[buffersize];

            int n = 0;
            int len;
            while  ((len = gzIn.read(buffer))  >   0 ) {
                out.write(buffer, 0, len);
            }
            out.close();
            gzIn.close();
            fin.close();
            return 0;
        } catch (FileNotFoundException ex) {
            Main.logger.error( ex );
        } catch (IOException ex) {
            Main.logger.error( ex );
        }
        return 1;
   }

    public void executeLoad() throws SQLException {

        startTime = new long[config_sites.length];

        while( flag ) {

            for ( int j = 0; j < config_sites.length; j++ ) {

                if ( startTime[j] == 0 )  startTime[j] = System.currentTimeMillis();
                
                //�����ѹ���ļ����ȶ�����н�ѹ
                if ( Boolean.parseBoolean(config_sites[j].get("compress")) ) {
                
                    FileList findziplist  = new FileList( config_sites[j].get("path").toString(), config_sites[j].get("compress_extend_name").toString());
                    File [] zipList = findziplist.FileLists;
                    for (int i = 0;i < zipList.length; i++ ) {
                        if (unzip(zipList[i].getAbsolutePath(),zipList[i].getAbsolutePath()+".csv")==0 ) {
                            boolean success = (new File( zipList[i].getAbsolutePath() )).delete();
                            if (!success) {
                                Main.logger.info( "Delete file " + zipList[i].getAbsolutePath() + "failed" );
                            }
                        } else {
                            Main.logger.info( "uncompress file " + zipList[i].getAbsolutePath() + "failed" );
                        }
                    }
                    
                }
                
                FileList findlist  = new FileList( config_sites[j].get("path").toString(), config_sites[j].get("extend_name").toString());
                File [] ArrList = findlist.FileLists;

                //���û��ɨ�赽�����ļ������˳�����ѭ��
                if ( ArrList.length == 0 ) continue;

                connect_db(config_sites[j].get("dbtype"),
                           config_sites[j].get("host"),
                           config_sites[j].get("database"),
                           config_sites[j].get("port"),
                           config_sites[j].get("username"),
                           config_sites[j].get("password")
                           );

                java.sql.Statement stmt = null;
                stmt = conn.createStatement();

                for (int i = 0;i < ArrList.length; i++ ) {

                    String sql = "";
                    if ( config_sites[j].get("dbtype").equals("mysql") ) {
                        sql = "LOAD DATA INFILE '" + ArrList[i].getAbsolutePath() + "' INTO TABLE "
                              + config_sites[j].get("table") + " FIELDS TERMINATED BY ',';";
                    } else {
                        Date now=new Date();
                        SimpleDateFormat f=new SimpleDateFormat("yyyy.MM.dd_");

                        sql = "COPY "+ config_sites[j].get("table") + " FROM '" + ArrList[i].getAbsolutePath()
                              + "' DELIMITER ',' ESCAPE AS '\"' EXCEPTIONS 'except." + config_sites[j].get("table") + "."+ f.format(now)
                              +".log' REJECTED DATA 'rejects."+ config_sites[j].get("table") + "." + f.format(now) +".log';";
                    }

                    Main.logger.info(config_sites[j].get("name").toString() + ":" + sql);
                    try {
                        stmt.setQueryTimeout(180);
                        stmt.execute(sql);
                        boolean success = (new File( ArrList[i].getAbsolutePath() )).delete();
                        if (!success) {
                            Main.logger.info( "Delete file " + ArrList[i].getAbsolutePath() + "failed" );
                        }
                        startTime[j] = System.currentTimeMillis();

                    } catch (SQLException sQLException) {
                        Main.logger.error( sQLException );
                    }

                    if ( !flag ) continue ;

                }
                stmt.close();
                conn.close();
                Main.logger.info( "disconnect mysql host:" + config_sites[j].get("host") + ",database:" + config_sites[j].get("database") );

                if ( !flag ) continue ;
            }

            if ( !flag ) continue ;

            //��ͣdealy��
            try {
                Main.logger.info( "Sleep Time:" + dealy + "s." );
                Thread.sleep ( dealy * 1000 ) ;
            } catch (InterruptedException ie) {
                Main.logger.error( ie );
            }

        }

    }

    private void connect_db( String dbtype , String host, String dbname, String port, String username, String password){
        try {
            Main.logger.info( "connecting to " + dbtype + " database server:" + host + ",database name:" + dbname );
            if ( !dbtype.equals("mysql") && !dbtype.equals("vertica") ) {
                Main.logger.error( "dbtype error!" );
            }

            if ( dbtype.equals("vertica") )
            {
                try {
                        Class.forName("com.vertica.Driver");
                    } catch (ClassNotFoundException e) {
                        Main.logger.error("Could not find the JDBC driver class.");
                        Main.logger.error(e.getMessage());
                    }
            }

            conn = DriverManager.getConnection("jdbc:" + dbtype +  "://" + host + ":" + port + "/" + dbname + "?user=" +
                    username + "&password=" + password);

        } catch (SQLException sQLException) {
            Main.logger.error( sQLException );
        }
    }

    public void loadConfig(){

        config_opt  = new HashMap<String,String>();

        Main.logger.info( "Loading config.xml...");
        viewXML("./config.xml");

        dealy       = Integer.parseInt( config_opt.get("dealy").toString() );

        Set<String> keys = config_opt.keySet();
        for(String key: keys) {
            Main.logger.info( key + " == " + config_opt.get(key));
        }

        for ( int i = 0; i < config_sites.length; i++ ) {
            Main.logger.info( "site " + i + ":");
            keys = config_sites[i].keySet();
            for(String key: keys) {
                Main.logger.info( key + " == " + config_sites[i].get(key));
            }
        }
    }

    // �÷��������XML�ļ������ݶ�����ת��HashMap
    public void viewXML(String xmlFile) {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = null;

        try {
            db = dbf.newDocumentBuilder();
        } catch (ParserConfigurationException ex) {
            Main.logger.error( ex );
        }

        try {
            doc = db.parse(new File(xmlFile));
        } catch (SAXException ex) {
            Main.logger.error( ex );
        } catch (IOException ex) {
            Main.logger.error( ex );
        }

        // ��xml�ļ���,ֻ��һ����Ԫ��,�ȰѸ�Ԫ���ó�������
        //Element element = doc.getDocumentElement();
        //System.out.println("��Ԫ��Ϊ:" + element.getTagName());
        // ��xml�ļ���,ֻ��һ����Ԫ��,�ȰѸ�Ԫ���ó�������
        //Element element = doc.getDocumentElement();
        //System.out.println("��Ԫ��Ϊ:" + element.getTagName());


        // ��ȡȫ��ѡ������HashMap config_opt��
        NodeList nodeList = doc.getElementsByTagName("section");
        //System.out.println("section�ڵ����ĳ���:" + nodeList.getLength());

        Node fatherNode = nodeList.item(0);
//        System.out.println("���ڵ�Ϊ:" + fatherNode.getNodeName());
//
//        �Ѹ��ڵ�������ó���
//        NamedNodeMap attributes = fatherNode.getAttributes();
//        for (int i = 0; i < attributes.getLength(); i++) {
//            Node attribute = attributes.item(i);
//            System.out.println("book��������Ϊ:" + attribute.getNodeName()
//            + " ���Ӧ������ֵΪ:" + attribute.getNodeValue());
//        }

        NodeList childNodes = fatherNode.getChildNodes();
        //System.out.println(childNodes.getLength());
        for (int j = 0; j < childNodes.getLength(); j++) {
            Node childNode = childNodes.item(j);
            // �������ڵ�����Element ,�ٽ���ȡֵ
            if (childNode instanceof Element) {
                //System.out.println("�ӽڵ���Ϊ:" + childNode.getNodeName() + "" + "���Ӧ��ֵΪ" + childNode.getFirstChild().getNodeValue());
                config_opt.put(childNode.getNodeName(), childNode.getFirstChild().getNodeValue());
            }
        }

        //��ȡsites��ѡ�񣬴����config_sites��
        NodeList nList = doc.getElementsByTagName("site");
        //System.out.println("site�ڵ����ĳ���:" + nList.getLength());

        config_sites = new HashMap[nList.getLength()];
        for ( int i = 0; i< nList.getLength(); i++ ){
            Node fNode = nList.item(i);
            NodeList cNodes = fNode.getChildNodes();

            config_sites[i] = new HashMap<String,String>();
            for (int j = 0; j < cNodes.getLength(); j++) {
                Node cNode = cNodes.item(j);
                // �������ڵ�����Element ,�ٽ���ȡֵ
                if (cNode instanceof Element) {
                    //System.out.println("�ӽڵ���Ϊ:" + cNode.getNodeName() + "" + "���Ӧ��ֵΪ" + cNode.getFirstChild().getNodeValue());
                    config_sites[i].put(cNode.getNodeName(), cNode.getFirstChild().getNodeValue());
                }
            }
        }

    }
}
